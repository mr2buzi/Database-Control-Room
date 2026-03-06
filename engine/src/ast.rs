use std::fmt::{Display, Formatter};

use crate::common::{ColumnDef, Filter, Value};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Statement {
    CreateTable(CreateTableStatement),
    Insert(InsertStatement),
    Select(SelectStatement),
    Delete(DeleteStatement),
    CreateIndex(CreateIndexStatement),
    Explain(Box<SelectStatement>),
    Begin,
    Commit,
    Rollback,
    MetaCommand(MetaCommand),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateTableStatement {
    pub table_name: String,
    pub columns: Vec<ColumnDef>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InsertStatement {
    pub table_name: String,
    pub values: Vec<Value>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SelectStatement {
    pub table_name: String,
    pub projection: Projection,
    pub filter: Option<Filter>,
    pub limit: Option<usize>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Projection {
    All,
    Columns(Vec<String>),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeleteStatement {
    pub table_name: String,
    pub filter: Option<Filter>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateIndexStatement {
    pub index_name: String,
    pub table_name: String,
    pub column_name: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MetaCommand {
    Exit,
}

impl Display for Statement {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::CreateTable(statement) => {
                writeln!(f, "CreateTable")?;
                writeln!(f, "  table: {}", statement.table_name)?;
                writeln!(f, "  columns:")?;
                for column in &statement.columns {
                    writeln!(f, "    - {} {}", column.name, column.column_type.as_str())?;
                }
                Ok(())
            }
            Self::Insert(statement) => {
                writeln!(f, "Insert")?;
                writeln!(f, "  table: {}", statement.table_name)?;
                writeln!(f, "  values:")?;
                for value in &statement.values {
                    writeln!(f, "    - {}", value.display_value())?;
                }
                Ok(())
            }
            Self::Select(statement) => {
                writeln!(f, "Select")?;
                writeln!(f, "  table: {}", statement.table_name)?;
                match &statement.projection {
                    Projection::All => writeln!(f, "  projection: *")?,
                    Projection::Columns(columns) => {
                        writeln!(f, "  projection: {}", columns.join(", "))?
                    }
                };
                if let Some(filter) = &statement.filter {
                    writeln!(
                        f,
                        "  filter: {} = {}",
                        filter.column,
                        filter.value.display_value()
                    )?;
                }
                if let Some(limit) = statement.limit {
                    writeln!(f, "  limit: {limit}")?;
                }
                Ok(())
            }
            Self::Delete(statement) => {
                writeln!(f, "Delete")?;
                writeln!(f, "  table: {}", statement.table_name)?;
                if let Some(filter) = &statement.filter {
                    writeln!(
                        f,
                        "  filter: {} = {}",
                        filter.column,
                        filter.value.display_value()
                    )?;
                }
                Ok(())
            }
            Self::CreateIndex(statement) => {
                writeln!(f, "CreateIndex")?;
                writeln!(f, "  index: {}", statement.index_name)?;
                writeln!(f, "  table: {}", statement.table_name)?;
                writeln!(f, "  column: {}", statement.column_name)
            }
            Self::Explain(statement) => {
                writeln!(f, "Explain")?;
                write!(f, "{}", Statement::Select((**statement).clone()))
            }
            Self::Begin => write!(f, "Begin"),
            Self::Commit => write!(f, "Commit"),
            Self::Rollback => write!(f, "Rollback"),
            Self::MetaCommand(command) => match command {
                MetaCommand::Exit => write!(f, "MetaCommand(.exit)"),
            },
        }
    }
}
