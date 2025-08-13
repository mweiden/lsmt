use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;
use sqlparser::ast::Statement;

pub struct SqlEngine {
    dialect: GenericDialect,
}

impl SqlEngine {
    pub fn new() -> Self {
        Self { dialect: GenericDialect {} }
    }

    pub fn parse(&self, sql: &str) -> Result<Vec<Statement>, sqlparser::parser::ParserError> {
        Parser::parse_sql(&self.dialect, sql)
    }
}
