use std::fmt::{Display, Formatter};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Error {
    Message(String),
    Io(String),
    Parse(String),
    Catalog(String),
    Storage(String),
    Execution(String),
    Transaction(String),
}

impl Error {
    pub fn message(message: impl Into<String>) -> Self {
        Self::Message(message.into())
    }
}

impl Display for Error {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Message(message)
            | Self::Io(message)
            | Self::Parse(message)
            | Self::Catalog(message)
            | Self::Storage(message)
            | Self::Execution(message)
            | Self::Transaction(message) => f.write_str(message),
        }
    }
}

impl std::error::Error for Error {}

impl From<std::io::Error> for Error {
    fn from(value: std::io::Error) -> Self {
        Self::Io(value.to_string())
    }
}

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum ColumnType {
    Int,
    Text,
}

impl ColumnType {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Int => "INT",
            Self::Text => "TEXT",
        }
    }

    pub fn to_tag(self) -> u8 {
        match self {
            Self::Int => 1,
            Self::Text => 2,
        }
    }

    pub fn from_tag(tag: u8) -> Result<Self> {
        match tag {
            1 => Ok(Self::Int),
            2 => Ok(Self::Text),
            _ => Err(Error::Catalog(format!("unknown column type tag {tag}"))),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct ColumnDef {
    pub name: String,
    pub column_type: ColumnType,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub enum Value {
    Int(i64),
    Text(String),
}

impl Value {
    pub fn value_type(&self) -> ColumnType {
        match self {
            Self::Int(_) => ColumnType::Int,
            Self::Text(_) => ColumnType::Text,
        }
    }

    pub fn display_value(&self) -> String {
        match self {
            Self::Int(value) => value.to_string(),
            Self::Text(value) => value.clone(),
        }
    }

    pub fn to_json(&self) -> String {
        match self {
            Self::Int(value) => value.to_string(),
            Self::Text(value) => format!("\"{}\"", escape_json(value)),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FilterOp {
    Eq,
    Gt,
    Gte,
    Lt,
    Lte,
}

impl FilterOp {
    pub fn symbol(self) -> &'static str {
        match self {
            Self::Eq => "=",
            Self::Gt => ">",
            Self::Gte => ">=",
            Self::Lt => "<",
            Self::Lte => "<=",
        }
    }

    pub fn matches(self, left: &Value, right: &Value) -> bool {
        if left.value_type() != right.value_type() {
            return false;
        }
        match self {
            Self::Eq => left == right,
            Self::Gt => left > right,
            Self::Gte => left >= right,
            Self::Lt => left < right,
            Self::Lte => left <= right,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct RowId {
    pub page_id: u32,
    pub slot_id: u16,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Filter {
    pub column: String,
    pub op: FilterOp,
    pub value: Value,
}

pub fn escape_json(input: &str) -> String {
    let mut escaped = String::new();
    for character in input.chars() {
        match character {
            '"' => escaped.push_str("\\\""),
            '\\' => escaped.push_str("\\\\"),
            '\n' => escaped.push_str("\\n"),
            '\r' => escaped.push_str("\\r"),
            '\t' => escaped.push_str("\\t"),
            control if control.is_control() => {
                escaped.push_str(&format!("\\u{:04x}", control as u32));
            }
            other => escaped.push(other),
        }
    }
    escaped
}
