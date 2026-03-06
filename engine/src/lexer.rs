use crate::common::{Error, Result};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TokenKind {
    Keyword(String),
    Identifier(String),
    Integer(i64),
    StringLiteral(String),
    Comma,
    LeftParen,
    RightParen,
    Equals,
    GreaterThan,
    GreaterThanOrEqual,
    LessThan,
    LessThanOrEqual,
    Semicolon,
    Star,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Token {
    pub kind: TokenKind,
}

pub fn lex(input: &str) -> Result<Vec<Token>> {
    let mut tokens = Vec::new();
    let characters: Vec<char> = input.chars().collect();
    let mut index = 0usize;

    while index < characters.len() {
        let current = characters[index];
        if current.is_whitespace() {
            index += 1;
            continue;
        }

        match current {
            ',' => {
                tokens.push(Token {
                    kind: TokenKind::Comma,
                });
                index += 1;
            }
            '(' => {
                tokens.push(Token {
                    kind: TokenKind::LeftParen,
                });
                index += 1;
            }
            ')' => {
                tokens.push(Token {
                    kind: TokenKind::RightParen,
                });
                index += 1;
            }
            '=' => {
                tokens.push(Token {
                    kind: TokenKind::Equals,
                });
                index += 1;
            }
            '>' => {
                if matches!(characters.get(index + 1), Some('=')) {
                    tokens.push(Token {
                        kind: TokenKind::GreaterThanOrEqual,
                    });
                    index += 2;
                } else {
                    tokens.push(Token {
                        kind: TokenKind::GreaterThan,
                    });
                    index += 1;
                }
            }
            '<' => {
                if matches!(characters.get(index + 1), Some('=')) {
                    tokens.push(Token {
                        kind: TokenKind::LessThanOrEqual,
                    });
                    index += 2;
                } else {
                    tokens.push(Token {
                        kind: TokenKind::LessThan,
                    });
                    index += 1;
                }
            }
            ';' => {
                tokens.push(Token {
                    kind: TokenKind::Semicolon,
                });
                index += 1;
            }
            '*' => {
                tokens.push(Token {
                    kind: TokenKind::Star,
                });
                index += 1;
            }
            '\'' => {
                index += 1;
                let start = index;
                while index < characters.len() && characters[index] != '\'' {
                    index += 1;
                }
                if index >= characters.len() {
                    return Err(Error::Parse("unterminated string literal".into()));
                }
                let value: String = characters[start..index].iter().collect();
                tokens.push(Token {
                    kind: TokenKind::StringLiteral(value),
                });
                index += 1;
            }
            '-' | '0'..='9' => {
                let start = index;
                index += 1;
                while index < characters.len() && characters[index].is_ascii_digit() {
                    index += 1;
                }
                let value: String = characters[start..index].iter().collect();
                let parsed = value
                    .parse::<i64>()
                    .map_err(|_| Error::Parse(format!("invalid integer literal {value}")))?;
                tokens.push(Token {
                    kind: TokenKind::Integer(parsed),
                });
            }
            '.' => {
                return Err(Error::Parse("meta commands are parsed before lexing".into()));
            }
            _ if is_identifier_start(current) => {
                let start = index;
                index += 1;
                while index < characters.len() && is_identifier_continue(characters[index]) {
                    index += 1;
                }
                let raw: String = characters[start..index].iter().collect();
                let upper = raw.to_ascii_uppercase();
                let kind = if is_keyword(&upper) {
                    TokenKind::Keyword(upper)
                } else {
                    TokenKind::Identifier(raw)
                };
                tokens.push(Token { kind });
            }
            _ => {
                return Err(Error::Parse(format!("unexpected character '{current}'")));
            }
        }
    }

    Ok(tokens)
}

fn is_identifier_start(character: char) -> bool {
    character.is_ascii_alphabetic() || character == '_'
}

fn is_identifier_continue(character: char) -> bool {
    character.is_ascii_alphanumeric() || character == '_'
}

fn is_keyword(keyword: &str) -> bool {
    matches!(
        keyword,
        "CREATE"
            | "TABLE"
            | "INSERT"
            | "INTO"
            | "VALUES"
            | "SELECT"
            | "FROM"
            | "WHERE"
            | "DELETE"
            | "INDEX"
            | "ON"
            | "EXPLAIN"
            | "BEGIN"
            | "COMMIT"
            | "ROLLBACK"
            | "LIMIT"
            | "INT"
            | "TEXT"
    )
}
