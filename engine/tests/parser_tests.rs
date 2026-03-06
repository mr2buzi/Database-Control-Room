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
fn parses_multiple_statements() {
    let statements = parse_statements("BEGIN; INSERT INTO users VALUES (1, 'Ana'); ROLLBACK;").unwrap();
    assert_eq!(statements.len(), 3);
    assert!(matches!(statements[0], Statement::Begin));
    assert!(matches!(statements[1], Statement::Insert(_)));
    assert!(matches!(statements[2], Statement::Rollback));
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
