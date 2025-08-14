//! Minimal SQL execution engine for the key-value store.

use sqlparser::ast::{BinaryOperator, Expr, SetExpr, Statement, Value};
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;

use crate::{storage::Storage, Database};

/// Errors that can occur when parsing or executing a query.
#[derive(thiserror::Error, Debug)]
pub enum QueryError {
    /// The input SQL could not be parsed.
    #[error("parse: {0}")]
    Parse(#[from] sqlparser::parser::ParserError),
    /// The query is syntactically valid but not supported by the engine.
    #[error("unsupported query")]
    Unsupported,
}

/// Simple SQL engine capable of executing a subset of INSERT and SELECT
/// statements.
pub struct SqlEngine {
    dialect: GenericDialect,
}

impl SqlEngine {
    /// Create a new [`SqlEngine`].
    pub fn new() -> Self {
        Self { dialect: GenericDialect {} }
    }

    /// Parse `sql` into a list of statements.
    pub fn parse(&self, sql: &str) -> Result<Vec<Statement>, sqlparser::parser::ParserError> {
        Parser::parse_sql(&self.dialect, sql)
    }

    /// Execute `sql` against the provided [`Database`].
    pub async fn execute<S: Storage + Sync + Send>(
        &self,
        db: &Database<S>,
        sql: &str,
    ) -> Result<Option<Vec<u8>>, QueryError> {
        let stmts = self.parse(sql)?;
        let mut result = None;
        for stmt in stmts {
            match stmt {
                Statement::Insert(insert) => {
                    let source = insert.source.ok_or(QueryError::Unsupported)?;
                    let values = match *source.body {
                        SetExpr::Values(v) => v,
                        _ => return Err(QueryError::Unsupported),
                    };
                    let row = values.rows.get(0).ok_or(QueryError::Unsupported)?;
                    if row.len() != 2 {
                        return Err(QueryError::Unsupported);
                    }
                    let key = match &row[0] {
                        Expr::Value(v) => match &v.value {
                            Value::SingleQuotedString(s) => s.clone(),
                            _ => return Err(QueryError::Unsupported),
                        },
                        _ => return Err(QueryError::Unsupported),
                    };
                    let val = match &row[1] {
                        Expr::Value(v) => match &v.value {
                            Value::SingleQuotedString(s) => s.clone(),
                            _ => return Err(QueryError::Unsupported),
                        },
                        _ => return Err(QueryError::Unsupported),
                    };
                    db.insert(key, val.into_bytes()).await;
                }
                Statement::Query(q) => {
                    match *q.body {
                        SetExpr::Select(select) => {
                            if let Some(cond) = select.selection {
                                if let Expr::BinaryOp { left, op, right } = cond {
                                    if op == BinaryOperator::Eq {
                                        if let (
                                            Expr::Identifier(id),
                                            Expr::Value(v),
                                        ) = (*left, *right)
                                        {
                                            if id.value.to_lowercase() == "key" {
                                                if let Value::SingleQuotedString(s) = v.value {
                                                    result = db.get(&s).await;
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                        _ => return Err(QueryError::Unsupported),
                    }
                }
                _ => return Err(QueryError::Unsupported),
            }
        }
        Ok(result)
    }
}
