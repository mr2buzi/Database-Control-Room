use crate::ast::{
    CreateIndexStatement, CreateTableStatement, DeleteStatement, InsertStatement, MetaCommand,
    Projection, SelectStatement, Statement,
};
use crate::common::{ColumnDef, ColumnType, Error, Filter, FilterOp, Result, Value};
use crate::lexer::{lex, Token, TokenKind};

pub fn parse_statement(input: &str) -> Result<Statement> {
    let trimmed = input.trim();
    if trimmed.is_empty() {
        return Err(Error::Parse("empty statement".into()));
    }
    if trimmed.eq_ignore_ascii_case(".exit") {
        return Ok(Statement::MetaCommand(MetaCommand::Exit));
    }

    let mut parser = Parser::new(lex(trimmed)?);
    let statement = parser.parse()?;
    parser.consume_optional_semicolon();
    if !parser.is_done() {
        return Err(Error::Parse("unexpected tokens after statement".into()));
    }
    Ok(statement)
}

pub fn parse_statements(input: &str) -> Result<Vec<Statement>> {
    let raw_parts = split_statements(input);
    if raw_parts.is_empty() {
        return Err(Error::Parse("empty statement".into()));
    }
    raw_parts
        .iter()
        .map(|part| parse_statement(part))
        .collect::<Result<Vec<_>>>()
}

struct Parser {
    tokens: Vec<Token>,
    index: usize,
}

impl Parser {
    fn new(tokens: Vec<Token>) -> Self {
        Self { tokens, index: 0 }
    }

    fn parse(&mut self) -> Result<Statement> {
        if self.match_keyword("CREATE") {
            if self.match_keyword("TABLE") {
                return self.parse_create_table();
            }
            if self.match_keyword("INDEX") {
                return self.parse_create_index();
            }
            return Err(Error::Parse("expected TABLE or INDEX after CREATE".into()));
        }
        if self.match_keyword("INSERT") {
            return self.parse_insert();
        }
        if self.match_keyword("SELECT") {
            return self.parse_select_after_keyword();
        }
        if self.match_keyword("DELETE") {
            return self.parse_delete();
        }
        if self.match_keyword("EXPLAIN") {
            self.expect_keyword("SELECT")?;
            let select = match self.parse_select_after_keyword()? {
                Statement::Select(statement) => statement,
                _ => unreachable!(),
            };
            return Ok(Statement::Explain(Box::new(select)));
        }
        if self.match_keyword("BEGIN") {
            return Ok(Statement::Begin);
        }
        if self.match_keyword("COMMIT") {
            return Ok(Statement::Commit);
        }
        if self.match_keyword("ROLLBACK") {
            return Ok(Statement::Rollback);
        }
        Err(Error::Parse("unsupported statement".into()))
    }

    fn parse_create_table(&mut self) -> Result<Statement> {
        let table_name = self.expect_identifier()?;
        self.expect_symbol(TokenKind::LeftParen)?;
        let mut columns = Vec::new();
        loop {
            let name = self.expect_identifier()?;
            let column_type = if self.match_keyword("INT") {
                ColumnType::Int
            } else if self.match_keyword("TEXT") {
                ColumnType::Text
            } else {
                return Err(Error::Parse("expected INT or TEXT column type".into()));
            };
            columns.push(ColumnDef { name, column_type });
            if self.match_symbol(TokenKind::Comma) {
                continue;
            }
            break;
        }
        self.expect_symbol(TokenKind::RightParen)?;
        Ok(Statement::CreateTable(CreateTableStatement {
            table_name,
            columns,
        }))
    }

    fn parse_insert(&mut self) -> Result<Statement> {
        self.expect_keyword("INTO")?;
        let table_name = self.expect_identifier()?;
        self.expect_keyword("VALUES")?;
        self.expect_symbol(TokenKind::LeftParen)?;
        let mut values = Vec::new();
        loop {
            values.push(self.expect_literal()?);
            if self.match_symbol(TokenKind::Comma) {
                continue;
            }
            break;
        }
        self.expect_symbol(TokenKind::RightParen)?;
        Ok(Statement::Insert(InsertStatement { table_name, values }))
    }

    fn parse_select_after_keyword(&mut self) -> Result<Statement> {
        let projection = if self.match_symbol(TokenKind::Star) {
            Projection::All
        } else {
            let mut columns = Vec::new();
            loop {
                columns.push(self.expect_identifier()?);
                if self.match_symbol(TokenKind::Comma) {
                    continue;
                }
                break;
            }
            Projection::Columns(columns)
        };
        self.expect_keyword("FROM")?;
        let table_name = self.expect_identifier()?;
        let filter = if self.match_keyword("WHERE") {
            Some(self.parse_filter()?)
        } else {
            None
        };
        let limit = if self.match_keyword("LIMIT") {
            Some(self.expect_usize()?)
        } else {
            None
        };
        Ok(Statement::Select(SelectStatement {
            table_name,
            projection,
            filter,
            limit,
        }))
    }

    fn parse_delete(&mut self) -> Result<Statement> {
        self.expect_keyword("FROM")?;
        let table_name = self.expect_identifier()?;
        let filter = if self.match_keyword("WHERE") {
            Some(self.parse_filter()?)
        } else {
            None
        };
        Ok(Statement::Delete(DeleteStatement { table_name, filter }))
    }

    fn parse_create_index(&mut self) -> Result<Statement> {
        let index_name = self.expect_identifier()?;
        self.expect_keyword("ON")?;
        let table_name = self.expect_identifier()?;
        self.expect_symbol(TokenKind::LeftParen)?;
        let column_name = self.expect_identifier()?;
        self.expect_symbol(TokenKind::RightParen)?;
        Ok(Statement::CreateIndex(CreateIndexStatement {
            index_name,
            table_name,
            column_name,
        }))
    }

    fn parse_filter(&mut self) -> Result<Filter> {
        let column = self.expect_identifier()?;
        let op = self.expect_filter_op()?;
        let value = self.expect_literal()?;
        Ok(Filter { column, op, value })
    }

    fn expect_filter_op(&mut self) -> Result<FilterOp> {
        match self.advance() {
            Some(Token {
                kind: TokenKind::Equals,
            }) => Ok(FilterOp::Eq),
            Some(Token {
                kind: TokenKind::GreaterThan,
            }) => Ok(FilterOp::Gt),
            Some(Token {
                kind: TokenKind::GreaterThanOrEqual,
            }) => Ok(FilterOp::Gte),
            Some(Token {
                kind: TokenKind::LessThan,
            }) => Ok(FilterOp::Lt),
            Some(Token {
                kind: TokenKind::LessThanOrEqual,
            }) => Ok(FilterOp::Lte),
            _ => Err(Error::Parse("expected comparison operator".into())),
        }
    }

    fn expect_literal(&mut self) -> Result<Value> {
        match self.advance() {
            Some(Token {
                kind: TokenKind::Integer(value),
            }) => Ok(Value::Int(*value)),
            Some(Token {
                kind: TokenKind::StringLiteral(value),
            }) => Ok(Value::Text(value.clone())),
            _ => Err(Error::Parse("expected literal".into())),
        }
    }

    fn expect_usize(&mut self) -> Result<usize> {
        match self.advance() {
            Some(Token {
                kind: TokenKind::Integer(value),
            }) if *value >= 0 => Ok(*value as usize),
            _ => Err(Error::Parse("expected non-negative integer".into())),
        }
    }

    fn expect_identifier(&mut self) -> Result<String> {
        match self.advance() {
            Some(Token {
                kind: TokenKind::Identifier(value),
            }) => Ok(value.clone()),
            _ => Err(Error::Parse("expected identifier".into())),
        }
    }

    fn expect_keyword(&mut self, expected: &str) -> Result<()> {
        if self.match_keyword(expected) {
            Ok(())
        } else {
            Err(Error::Parse(format!("expected keyword {expected}")))
        }
    }

    fn expect_symbol(&mut self, expected: TokenKind) -> Result<()> {
        if self.match_symbol(expected.clone()) {
            Ok(())
        } else {
            Err(Error::Parse(format!("expected symbol {}", describe_symbol(&expected))))
        }
    }

    fn match_keyword(&mut self, expected: &str) -> bool {
        match self.peek() {
            Some(Token {
                kind: TokenKind::Keyword(keyword),
            }) if keyword == expected => {
                self.index += 1;
                true
            }
            _ => false,
        }
    }

    fn match_symbol(&mut self, expected: TokenKind) -> bool {
        match self.peek() {
            Some(token) if token.kind == expected => {
                self.index += 1;
                true
            }
            _ => false,
        }
    }

    fn consume_optional_semicolon(&mut self) {
        let _ = self.match_symbol(TokenKind::Semicolon);
    }

    fn peek(&self) -> Option<&Token> {
        self.tokens.get(self.index)
    }

    fn advance(&mut self) -> Option<&Token> {
        let token = self.tokens.get(self.index);
        if token.is_some() {
            self.index += 1;
        }
        token
    }

    fn is_done(&self) -> bool {
        self.index >= self.tokens.len()
    }
}

fn describe_symbol(kind: &TokenKind) -> &'static str {
    match kind {
        TokenKind::Comma => ",",
        TokenKind::LeftParen => "(",
        TokenKind::RightParen => ")",
        TokenKind::Equals => "=",
        TokenKind::GreaterThan => ">",
        TokenKind::GreaterThanOrEqual => ">=",
        TokenKind::LessThan => "<",
        TokenKind::LessThanOrEqual => "<=",
        TokenKind::Semicolon => ";",
        TokenKind::Star => "*",
        _ => "token",
    }
}

fn split_statements(input: &str) -> Vec<String> {
    let mut statements = Vec::new();
    let mut current = String::new();
    let mut in_string = false;
    let chars = input.chars().peekable();

    for character in chars {
        match character {
            '\'' => {
                in_string = !in_string;
                current.push(character);
            }
            ';' if !in_string => {
                if !current.trim().is_empty() {
                    statements.push(format!("{};", current.trim()));
                }
                current.clear();
            }
            other => current.push(other),
        }
    }

    if !current.trim().is_empty() {
        statements.push(current.trim().to_string());
    }

    statements
}
