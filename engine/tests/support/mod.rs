#![allow(dead_code)]

use std::fs;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use slatedb::executor::{Database, OutputFormat};
use slatedb::parser::parse_statement;

pub struct TestDb {
    root: PathBuf,
    pub path: PathBuf,
}

impl TestDb {
    pub fn new(prefix: &str) -> Self {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let root = std::env::temp_dir().join(format!("slatedb-tests-{prefix}-{unique}"));
        fs::create_dir_all(&root).unwrap();
        let path = root.join("main.sdb");
        Self { root, path }
    }
}

impl Drop for TestDb {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.root);
    }
}

pub fn exec(db: &mut Database, sql: &str, format: OutputFormat) -> String {
    db.execute_statement(parse_statement(sql).unwrap(), format, false)
        .unwrap()
        .rendered
}

pub fn open_db(path: &Path) -> Database {
    Database::open(path).unwrap()
}
