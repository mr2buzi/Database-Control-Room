mod support;

use slatedb::executor::OutputFormat;

use support::{exec, open_db, TestDb};

#[test]
fn indexed_range_scan_returns_rows_in_ascending_key_order() {
    let test_db = TestDb::new("range-order");
    let mut db = open_db(&test_db.path);
    exec(
        &mut db,
        "CREATE TABLE users (id INT, name TEXT, tier TEXT);",
        OutputFormat::Table,
    );
    exec(
        &mut db,
        "INSERT INTO users VALUES (1, 'Ana', 'free');",
        OutputFormat::Table,
    );
    exec(
        &mut db,
        "INSERT INTO users VALUES (3, 'Mia', 'pro');",
        OutputFormat::Table,
    );
    exec(
        &mut db,
        "INSERT INTO users VALUES (2, 'Jay', 'pro');",
        OutputFormat::Table,
    );
    exec(
        &mut db,
        "CREATE INDEX idx_users_id ON users(id);",
        OutputFormat::Table,
    );

    let result = exec(
        &mut db,
        "SELECT id, name FROM users WHERE id >= 2;",
        OutputFormat::Json,
    );

    let jay_index = result.find("[2,\"Jay\"]").unwrap();
    let mia_index = result.find("[3,\"Mia\"]").unwrap();
    assert!(jay_index < mia_index);
}

#[test]
fn range_scan_honors_inclusive_and_exclusive_bounds() {
    let test_db = TestDb::new("range-bounds");
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

    let gte = exec(
        &mut db,
        "SELECT id FROM users WHERE id >= 2;",
        OutputFormat::Json,
    );
    assert!(gte.contains("[2]"));
    assert!(gte.contains("[3]"));

    let gt = exec(
        &mut db,
        "SELECT id FROM users WHERE id > 2;",
        OutputFormat::Json,
    );
    assert!(!gt.contains("[2]"));
    assert!(gt.contains("[3]"));

    let lte = exec(
        &mut db,
        "SELECT id FROM users WHERE id <= 2;",
        OutputFormat::Json,
    );
    assert!(lte.contains("[1]"));
    assert!(lte.contains("[2]"));
    assert!(!lte.contains("[3]"));
}

#[test]
fn range_scan_skips_tombstoned_rows_and_stops_at_limit() {
    let test_db = TestDb::new("range-limit");
    let mut db = open_db(&test_db.path);
    exec(&mut db, "CREATE TABLE users (id INT, name TEXT);", OutputFormat::Table);
    exec(&mut db, "INSERT INTO users VALUES (1, 'Ana');", OutputFormat::Table);
    exec(&mut db, "INSERT INTO users VALUES (2, 'Jay');", OutputFormat::Table);
    exec(&mut db, "INSERT INTO users VALUES (3, 'Mia');", OutputFormat::Table);
    exec(&mut db, "INSERT INTO users VALUES (4, 'Theo');", OutputFormat::Table);
    exec(
        &mut db,
        "CREATE INDEX idx_users_id ON users(id);",
        OutputFormat::Table,
    );
    exec(
        &mut db,
        "DELETE FROM users WHERE id = 3;",
        OutputFormat::Table,
    );

    let result = exec(
        &mut db,
        "SELECT id, name FROM users WHERE id >= 2 LIMIT 2;",
        OutputFormat::Json,
    );

    assert!(result.contains("\"rows_read\":2"));
    assert!(result.contains("\"rows_returned\":2"));
    assert!(result.contains("[2,\"Jay\"]"));
    assert!(!result.contains("[3,\"Mia\"]"));
    assert!(result.contains("[4,\"Theo\"]"));
}
