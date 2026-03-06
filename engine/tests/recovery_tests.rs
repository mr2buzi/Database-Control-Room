mod support;

use std::fs;

use slatedb::executor::OutputFormat;

use support::{exec, open_db, TestDb};

#[test]
fn rollback_discards_uncommitted_rows() {
    let test_db = TestDb::new("rollback");
    {
        let mut db = open_db(&test_db.path);
        exec(&mut db, "CREATE TABLE users (id INT, name TEXT);", OutputFormat::Table);
        exec(&mut db, "BEGIN;", OutputFormat::Table);
        exec(&mut db, "INSERT INTO users VALUES (1, 'Ana');", OutputFormat::Table);
        exec(&mut db, "ROLLBACK;", OutputFormat::Table);
    }

    let mut reopened = open_db(&test_db.path);
    let json = exec(
        &mut reopened,
        "SELECT id, name FROM users;",
        OutputFormat::Json,
    );
    assert!(json.contains("\"rows\":[]"));
}

#[test]
fn committed_transaction_survives_restart() {
    let test_db = TestDb::new("commit");
    {
        let mut db = open_db(&test_db.path);
        exec(&mut db, "CREATE TABLE users (id INT, name TEXT);", OutputFormat::Table);
        exec(&mut db, "BEGIN;", OutputFormat::Table);
        exec(&mut db, "INSERT INTO users VALUES (1, 'Ana');", OutputFormat::Table);
        exec(&mut db, "COMMIT;", OutputFormat::Table);
    }

    let mut reopened = open_db(&test_db.path);
    let json = exec(
        &mut reopened,
        "SELECT id, name FROM users WHERE id = 1;",
        OutputFormat::Json,
    );
    assert!(json.contains("\"rows\":[[1,\"Ana\"]]"));
}

#[test]
fn recovery_replays_wal_after_data_file_is_reverted() {
    let test_db = TestDb::new("recovery");
    {
        let mut db = open_db(&test_db.path);
        exec(&mut db, "CREATE TABLE users (id INT, name TEXT);", OutputFormat::Table);
    }

    let snapshot = fs::read(&test_db.path).unwrap();

    {
        let mut db = open_db(&test_db.path);
        exec(&mut db, "INSERT INTO users VALUES (9, 'Mia');", OutputFormat::Table);
    }

    fs::write(&test_db.path, snapshot).unwrap();

    let mut reopened = open_db(&test_db.path);
    let json = exec(
        &mut reopened,
        "SELECT id, name FROM users WHERE id = 9;",
        OutputFormat::Json,
    );
    assert!(json.contains("\"rows\":[[9,\"Mia\"]]"));
}
