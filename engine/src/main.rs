use std::env;
use std::path::PathBuf;

use slatedb::common::{Error, Result};
use slatedb::executor::{render_json_error, run_benchmark, Database, OutputFormat};
use slatedb::repl::run_repl;

fn main() {
    if let Err(error) = try_main() {
        eprintln!("error: {error}");
        std::process::exit(1);
    }
}

fn try_main() -> Result<()> {
    let mut args = env::args().skip(1);
    let command = args
        .next()
        .ok_or_else(|| Error::message("usage: slatedb <repl|exec|inspect|bench> --data <path>"))?;
    match command.as_str() {
        "repl" => {
            let mut data_path = None;
            let mut debug_ast = false;
            while let Some(argument) = args.next() {
                match argument.as_str() {
                    "--data" => data_path = args.next().map(PathBuf::from),
                    "--debug-ast" => debug_ast = true,
                    other => return Err(Error::message(format!("unknown argument {other}"))),
                }
            }
            run_repl(&require_path(data_path, "--data")?, debug_ast)
        }
        "exec" => {
            let mut data_path = None;
            let mut query = None;
            let mut format = OutputFormat::Table;
            while let Some(argument) = args.next() {
                match argument.as_str() {
                    "--data" => data_path = args.next().map(PathBuf::from),
                    "--query" => query = args.next(),
                    "--format" => {
                        let raw = args
                            .next()
                            .ok_or_else(|| Error::message("missing format value"))?;
                        format = match raw.as_str() {
                            "table" => OutputFormat::Table,
                            "json" => OutputFormat::Json,
                            _ => return Err(Error::message("format must be table or json")),
                        };
                    }
                    other => return Err(Error::message(format!("unknown argument {other}"))),
                }
            }
            let mut database = Database::open(&require_path(data_path, "--data")?)?;
            let query = query.ok_or_else(|| Error::message("missing --query argument"))?;
            match database.execute_query_text(&query, format) {
                Ok(output) => {
                    print!("{}", output.rendered);
                    Ok(())
                }
                Err(error) if matches!(format, OutputFormat::Json) => {
                    print!("{}", render_json_error(&error));
                    Ok(())
                }
                Err(error) => Err(error),
            }
        }
        "inspect" => {
            let mut data_path = None;
            while let Some(argument) = args.next() {
                match argument.as_str() {
                    "--data" => data_path = args.next().map(PathBuf::from),
                    other => return Err(Error::message(format!("unknown argument {other}"))),
                }
            }
            let mut database = Database::open(&require_path(data_path, "--data")?)?;
            print!("{}", database.inspect_schema_json()?);
            Ok(())
        }
        "bench" => {
            let mut data_path = None;
            while let Some(argument) = args.next() {
                match argument.as_str() {
                    "--data" => data_path = args.next().map(PathBuf::from),
                    other => return Err(Error::message(format!("unknown argument {other}"))),
                }
            }
            println!("{}", run_benchmark(&require_path(data_path, "--data")?)?);
            Ok(())
        }
        _ => Err(Error::message("usage: slatedb <repl|exec|inspect|bench> --data <path>")),
    }
}

fn require_path(value: Option<PathBuf>, flag: &str) -> Result<PathBuf> {
    value.ok_or_else(|| Error::message(format!("missing required argument {flag}")))
}
