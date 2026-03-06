mod support;

use slatedb::executor::OutputFormat;

use support::{open_db, TestDb};

#[test]
fn multi_statement_exec_uses_one_database_session() {
    let test_db = TestDb::new("multi");
    let mut db = open_db(&test_db.path);
    db.execute_query_text(
        "CREATE TABLE users (id INT, name TEXT, tier TEXT);",
        OutputFormat::Json,
    )
    .unwrap();

    let output = db
        .execute_query_text(
            "BEGIN; INSERT INTO users VALUES (5, 'Rina', 'pro'); ROLLBACK; SELECT id, name, tier FROM users WHERE id = 5;",
            OutputFormat::Json,
        )
        .unwrap()
        .rendered;

    assert!(output.contains("\"final\""));
    assert!(output.contains("\"rows\":[]"));
}

#[test]
fn inspect_schema_reports_live_catalog() {
    let test_db = TestDb::new("inspect");
    let mut db = open_db(&test_db.path);
    db.execute_query_text(
        "CREATE TABLE users (id INT, name TEXT); INSERT INTO users VALUES (1, 'Ana'); CREATE INDEX idx_users_id ON users(id);",
        OutputFormat::Json,
    )
    .unwrap();

    let schema = db.inspect_schema_json().unwrap();
    assert!(schema.contains("\"name\":\"users\""));
    assert!(schema.contains("\"rowCount\":1"));
    assert!(schema.contains("\"indexed\":true"));
}
