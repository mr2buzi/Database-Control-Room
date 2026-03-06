mod support;

use slatedb::executor::OutputFormat;

use support::{exec, open_db, TestDb};

#[test]
fn persists_rows_across_restart() {
    let test_db = TestDb::new("persist");
    {
        let mut db = open_db(&test_db.path);
        exec(&mut db, "CREATE TABLE users (id INT, name TEXT);", OutputFormat::Table);
        exec(&mut db, "INSERT INTO users VALUES (1, 'Ana');", OutputFormat::Table);
        exec(&mut db, "INSERT INTO users VALUES (2, 'Jay');", OutputFormat::Table);
    }

    let mut reopened = open_db(&test_db.path);
    let json = exec(
        &mut reopened,
        "SELECT id, name FROM users WHERE id = 2;",
        OutputFormat::Json,
    );
    assert!(json.contains("\"rows\":[[2,\"Jay\"]]"));
}

#[test]
fn delete_marks_row_invisible() {
    let test_db = TestDb::new("delete");
    let mut db = open_db(&test_db.path);
    exec(&mut db, "CREATE TABLE users (id INT, name TEXT);", OutputFormat::Table);
    exec(&mut db, "INSERT INTO users VALUES (1, 'Ana');", OutputFormat::Table);
    exec(&mut db, "INSERT INTO users VALUES (2, 'Jay');", OutputFormat::Table);
    exec(
        &mut db,
        "DELETE FROM users WHERE id = 1;",
        OutputFormat::Table,
    );

    let json = exec(
        &mut db,
        "SELECT id, name FROM users;",
        OutputFormat::Json,
    );
    assert!(json.contains("\"rows\":[[2,\"Jay\"]]"));
    assert!(!json.contains("\"Ana\""));
}

#[test]
fn oversized_row_is_rejected() {
    let test_db = TestDb::new("oversized");
    let mut db = open_db(&test_db.path);
    exec(&mut db, "CREATE TABLE docs (id INT, body TEXT);", OutputFormat::Table);
    let body = "x".repeat(5000);
    let sql = format!("INSERT INTO docs VALUES (1, '{}');", body);
    let error = db
        .execute_statement(slatedb::parser::parse_statement(&sql).unwrap(), OutputFormat::Table, false)
        .unwrap_err();
    assert!(error.to_string().contains("too large"));
}

#[test]
fn limit_restricts_rows_returned() {
    let test_db = TestDb::new("limit");
    let mut db = open_db(&test_db.path);
    exec(&mut db, "CREATE TABLE users (id INT, name TEXT);", OutputFormat::Table);
    exec(&mut db, "INSERT INTO users VALUES (1, 'Ana');", OutputFormat::Table);
    exec(&mut db, "INSERT INTO users VALUES (2, 'Jay');", OutputFormat::Table);
    exec(&mut db, "INSERT INTO users VALUES (3, 'Mia');", OutputFormat::Table);

    let json = exec(
        &mut db,
        "SELECT id, name FROM users LIMIT 2;",
        OutputFormat::Json,
    );
    assert!(json.contains("\"rows_returned\":2"));
}
