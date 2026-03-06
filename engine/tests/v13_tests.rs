mod support;

use slatedb::executor::OutputFormat;
use slatedb::parser::parse_statement;

use support::{exec, open_db, TestDb};

#[test]
fn update_persists_across_restart() {
    let test_db = TestDb::new("update-persist");
    {
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
            "UPDATE users SET tier = 'pro' WHERE id = 1;",
            OutputFormat::Table,
        );
    }

    let mut reopened = open_db(&test_db.path);
    let json = exec(
        &mut reopened,
        "SELECT id, name, tier FROM users WHERE id = 1;",
        OutputFormat::Json,
    );
    assert!(json.contains("[1,\"Ana\",\"pro\"]"));
}

#[test]
fn update_without_where_affects_all_live_rows() {
    let test_db = TestDb::new("update-all");
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
        "INSERT INTO users VALUES (2, 'Jay', 'free');",
        OutputFormat::Table,
    );

    let update = exec(
        &mut db,
        "UPDATE users SET tier = 'pro';",
        OutputFormat::Json,
    );
    assert!(update.contains("\"message\":\"2 row(s) updated\""));

    let json = exec(
        &mut db,
        "SELECT id, tier FROM users WHERE tier = 'pro';",
        OutputFormat::Json,
    );
    assert!(json.contains("[1,\"pro\"]"));
    assert!(json.contains("[2,\"pro\"]"));
}

#[test]
fn updating_indexed_column_is_visible_to_future_index_lookups() {
    let test_db = TestDb::new("update-indexed-column");
    let mut db = open_db(&test_db.path);
    exec(&mut db, "CREATE TABLE users (id INT, name TEXT);", OutputFormat::Table);
    exec(&mut db, "INSERT INTO users VALUES (1, 'Ana');", OutputFormat::Table);
    exec(&mut db, "CREATE INDEX idx_users_id ON users(id);", OutputFormat::Table);

    exec(
        &mut db,
        "UPDATE users SET id = 8 WHERE id = 1;",
        OutputFormat::Table,
    );

    let new_lookup = exec(
        &mut db,
        "SELECT id, name FROM users WHERE id = 8;",
        OutputFormat::Json,
    );
    assert!(new_lookup.contains("\"used_index\":true"));
    assert!(new_lookup.contains("[8,\"Ana\"]"));

    let old_lookup = exec(
        &mut db,
        "SELECT id, name FROM users WHERE id = 1;",
        OutputFormat::Json,
    );
    assert!(old_lookup.contains("\"rows\":[]"));
}

#[test]
fn rollback_discards_update_changes() {
    let test_db = TestDb::new("update-rollback");
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
    exec(&mut db, "BEGIN;", OutputFormat::Table);
    exec(
        &mut db,
        "UPDATE users SET tier = 'pro' WHERE id = 1;",
        OutputFormat::Table,
    );
    exec(&mut db, "ROLLBACK;", OutputFormat::Table);

    let json = exec(
        &mut db,
        "SELECT id, name, tier FROM users WHERE id = 1;",
        OutputFormat::Json,
    );
    assert!(json.contains("[1,\"Ana\",\"free\"]"));
}

#[test]
fn commit_persists_update_changes() {
    let test_db = TestDb::new("update-commit");
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
    exec(&mut db, "BEGIN;", OutputFormat::Table);
    exec(
        &mut db,
        "UPDATE users SET tier = 'pro' WHERE id = 1;",
        OutputFormat::Table,
    );
    exec(&mut db, "COMMIT;", OutputFormat::Table);

    let reopened = open_db(&test_db.path);
    let mut reopened = reopened;
    let json = exec(
        &mut reopened,
        "SELECT id, name, tier FROM users WHERE id = 1;",
        OutputFormat::Json,
    );
    assert!(json.contains("[1,\"Ana\",\"pro\"]"));
}

#[test]
fn failed_type_mismatch_update_leaves_rows_unchanged() {
    let test_db = TestDb::new("update-type-mismatch");
    let mut db = open_db(&test_db.path);
    exec(&mut db, "CREATE TABLE users (id INT, name TEXT);", OutputFormat::Table);
    exec(&mut db, "INSERT INTO users VALUES (1, 'Ana');", OutputFormat::Table);

    let error = db
        .execute_statement(
            parse_statement("UPDATE users SET id = 'bad' WHERE id = 1;").unwrap(),
            OutputFormat::Table,
            false,
        )
        .unwrap_err();
    assert!(error.to_string().contains("type mismatch"));

    let json = exec(
        &mut db,
        "SELECT id, name FROM users WHERE id = 1;",
        OutputFormat::Json,
    );
    assert!(json.contains("[1,\"Ana\"]"));
}

#[test]
fn failed_oversized_update_leaves_rows_unchanged_inside_transaction() {
    let test_db = TestDb::new("update-oversized");
    let mut db = open_db(&test_db.path);
    exec(&mut db, "CREATE TABLE docs (id INT, body TEXT);", OutputFormat::Table);
    exec(&mut db, "INSERT INTO docs VALUES (1, 'short');", OutputFormat::Table);
    exec(&mut db, "BEGIN;", OutputFormat::Table);

    let body = "x".repeat(5000);
    let sql = format!("UPDATE docs SET body = '{}' WHERE id = 1;", body);
    let error = db
        .execute_statement(parse_statement(&sql).unwrap(), OutputFormat::Table, false)
        .unwrap_err();
    assert!(error.to_string().contains("too large"));

    let json = exec(
        &mut db,
        "SELECT id, body FROM docs WHERE id = 1;",
        OutputFormat::Json,
    );
    assert!(json.contains("[1,\"short\"]"));
    exec(&mut db, "ROLLBACK;", OutputFormat::Table);
}
