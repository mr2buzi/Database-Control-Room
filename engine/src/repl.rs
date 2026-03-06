use std::io::{self, Write};
use std::path::Path;

use crate::ast::{MetaCommand, Statement};
use crate::common::Result;
use crate::executor::{Database, OutputFormat};
use crate::parser::parse_statement;

pub fn run_repl(data_path: &Path, debug_ast: bool) -> Result<()> {
    let mut database = Database::open(data_path)?;
    let stdin = io::stdin();

    loop {
        print!("slatedb> ");
        io::stdout().flush()?;
        let mut buffer = String::new();
        stdin.read_line(&mut buffer)?;
        if buffer.trim().is_empty() {
            continue;
        }
        let statement = match parse_statement(&buffer) {
            Ok(statement) => statement,
            Err(error) => {
                eprintln!("error: {error}");
                continue;
            }
        };

        if debug_ast {
            println!("{statement}");
        }

        if matches!(statement, Statement::MetaCommand(MetaCommand::Exit)) {
            break;
        }

        match database.execute_statement(statement, OutputFormat::Table, debug_ast) {
            Ok(output) => {
                if !output.rendered.is_empty() {
                    println!("{}", output.rendered);
                }
            }
            Err(error) => eprintln!("error: {error}"),
        }
    }

    Ok(())
}
