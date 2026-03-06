use slatedb::ast::{Projection, Statement};
use slatedb::parser::{parse_statement, parse_statements};

#[test]
fn parses_create_table() {
    let statement = parse_statement("CREATE TABLE users (id INT, name TEXT);").unwrap();
    match statement {
        Statement::CreateTable(create) => {
            assert_eq!(create.table_name, "users");
            assert_eq!(create.columns.len(), 2);
            assert_eq!(create.columns[0].name, "id");
            assert_eq!(create.columns[1].name, "name");
        }
        _ => panic!("expected create table"),
    }
}

#[test]
fn parses_select_with_filter() {
    let statement = parse_statement("SELECT id, name FROM users WHERE id = 2 LIMIT 1;").unwrap();
    match statement {
        Statement::Select(select) => {
            assert_eq!(select.table_name, "users");
            assert!(matches!(select.projection, Projection::Columns(_)));
            assert_eq!(select.filter.unwrap().column, "id");
            assert_eq!(select.limit, Some(1));
        }
        _ => panic!("expected select"),
    }
}

#[test]
fn parses_select_with_range_filter() {
    let statement = parse_statement("SELECT id FROM users WHERE id >= 2 LIMIT 2;").unwrap();
    match statement {
        Statement::Select(select) => {
            let filter = select.filter.unwrap();
            assert_eq!(filter.column, "id");
            assert_eq!(filter.op.symbol(), ">=");
            assert_eq!(select.limit, Some(2));
        }
        _ => panic!("expected select"),
    }
}

#[test]
fn parses_multiple_statements() {
    let statements = parse_statements("BEGIN; INSERT INTO users VALUES (1, 'Ana'); ROLLBACK;").unwrap();
    assert_eq!(statements.len(), 3);
    assert!(matches!(statements[0], Statement::Begin));
    assert!(matches!(statements[1], Statement::Insert(_)));
    assert!(matches!(statements[2], Statement::Rollback));
}

#[test]
fn parses_update_with_filter() {
    let statement = parse_statement("UPDATE users SET tier = 'pro' WHERE id >= 2;").unwrap();
    match statement {
        Statement::Update(update) => {
            assert_eq!(update.table_name, "users");
            assert_eq!(update.column_name, "tier");
            assert_eq!(update.filter.unwrap().op.symbol(), ">=");
        }
        _ => panic!("expected update"),
    }
}

#[test]
fn parses_update_without_where() {
    let statement = parse_statement("UPDATE users SET tier = 'pro';").unwrap();
    match statement {
        Statement::Update(update) => {
            assert_eq!(update.table_name, "users");
            assert_eq!(update.column_name, "tier");
            assert!(update.filter.is_none());
        }
        _ => panic!("expected update"),
    }
}

#[test]
fn parses_meta_command() {
    let statement = parse_statement(".exit").unwrap();
    assert!(matches!(statement, Statement::MetaCommand(_)));
}

#[test]
fn rejects_invalid_sql() {
    let error = parse_statement("SELECT FROM users;").unwrap_err();
    assert!(error.to_string().contains("expected identifier"));
}

#[test]
fn rejects_multi_predicate_where_clause() {
    let error = parse_statement("SELECT * FROM users WHERE id >= 2 AND id <= 4;").unwrap_err();
    assert!(error.to_string().contains("unexpected tokens after statement"));
}

#[test]
fn rejects_multi_assignment_update() {
    let error = parse_statement("UPDATE users SET tier = 'pro', name = 'Ana';").unwrap_err();
    assert!(error.to_string().contains("one SET assignment"));
}

#[test]
fn rejects_expression_update_value() {
    let error = parse_statement("UPDATE users SET id = id + 1;").unwrap_err();
    let message = error.to_string();
    assert!(message.contains("expected literal") || message.contains("unexpected character"));
}
