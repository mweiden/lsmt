//! Minimal SQL execution engine for the key-value store.

use std::collections::BTreeMap;

use sqlparser::ast::{
    Assignment, AssignmentTarget, BinaryOperator, DataType, Delete, Expr, FromTable, Function,
    FunctionArg, FunctionArgExpr, FunctionArguments, Insert, LimitClause, ObjectName, ObjectType,
    OrderBy, OrderByKind, Query, Select, SelectItem, SetExpr, Statement, TableFactor,
    TableWithJoins, Value,
};
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;

use crate::Database;

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

/// Simple SQL engine capable of executing a subset of SQL statements.
pub struct SqlEngine {
    dialect: GenericDialect,
}

impl SqlEngine {
    /// Create a new [`SqlEngine`].
    pub fn new() -> Self {
        Self {
            dialect: GenericDialect {},
        }
    }

    /// Parse `sql` into a list of statements.
    pub fn parse(&self, sql: &str) -> Result<Vec<Statement>, sqlparser::parser::ParserError> {
        Parser::parse_sql(&self.dialect, sql)
    }

    /// Execute `sql` against the provided [`Database`].
    pub async fn execute(&self, db: &Database, sql: &str) -> Result<Option<Vec<u8>>, QueryError> {
        let stmts = self.parse(sql)?;
        let mut result = None;
        for stmt in stmts {
            result = self.execute_stmt(db, stmt).await?;
        }
        Ok(result)
    }

    /// Execute a single parsed SQL [`Statement`].
    async fn execute_stmt(
        &self,
        db: &Database,
        stmt: Statement,
    ) -> Result<Option<Vec<u8>>, QueryError> {
        match stmt {
            Statement::Insert(insert) => {
                self.exec_insert(db, insert).await?;
                Ok(None)
            }
            Statement::Update {
                table,
                assignments,
                selection,
                ..
            } => {
                self.exec_update(db, table, assignments, selection).await?;
                Ok(None)
            }
            Statement::Delete(delete) => {
                self.exec_delete(db, delete).await?;
                Ok(None)
            }
            Statement::CreateTable(ct) => {
                let ns = object_name_to_ns(&ct.name).ok_or(QueryError::Unsupported)?;
                register_table(db, &ns).await;
                Ok(None)
            }
            Statement::ShowTables { .. } => self.exec_show_tables(db).await,
            Statement::Drop {
                object_type: ObjectType::Table,
                names,
                ..
            } => {
                if let Some(name) = names.first() {
                    let ns = object_name_to_ns(name).ok_or(QueryError::Unsupported)?;
                    db.clear_ns(&ns).await;
                    db.delete_ns("_tables", &ns).await;
                    Ok(None)
                } else {
                    Err(QueryError::Unsupported)
                }
            }
            Statement::Query(q) => self.exec_query(db, q).await,
            _ => Err(QueryError::Unsupported),
        }
    }

    /// Handle an `INSERT` statement inserting a single key/value pair.
    async fn exec_insert(&self, db: &Database, insert: Insert) -> Result<(), QueryError> {
        // Extract the target namespace/table name.
        let ns = match &insert.table {
            sqlparser::ast::TableObject::TableName(name) => {
                object_name_to_ns(name).ok_or(QueryError::Unsupported)?
            }
            _ => return Err(QueryError::Unsupported),
        };
        register_table(db, &ns).await;
        let source = insert.source.ok_or(QueryError::Unsupported)?;
        let values = match *source.body {
            SetExpr::Values(v) => v,
            _ => return Err(QueryError::Unsupported),
        };
        let row = values.rows.get(0).ok_or(QueryError::Unsupported)?;
        if row.len() != 2 {
            return Err(QueryError::Unsupported);
        }
        // Parse key and value expressions expecting single quoted strings.
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
        db.insert_ns(&ns, key, val.into_bytes()).await;
        Ok(())
    }

    /// Handle an `UPDATE` statement that sets the value for a single key.
    async fn exec_update(
        &self,
        db: &Database,
        table: TableWithJoins,
        assignments: Vec<Assignment>,
        selection: Option<Expr>,
    ) -> Result<(), QueryError> {
        let ns = table_factor_to_ns(&table.relation).ok_or(QueryError::Unsupported)?;
        register_table(db, &ns).await;
        // Extract the key from the WHERE clause; only equality is supported.
        let key = if let Some(expr) = selection {
            if let Expr::BinaryOp { left, op, right } = expr {
                if op == BinaryOperator::Eq {
                    if let (Expr::Identifier(id), Expr::Value(v)) = (*left, *right) {
                        if id.value.to_lowercase() == "key" {
                            if let Value::SingleQuotedString(s) = v.value {
                                s
                            } else {
                                return Err(QueryError::Unsupported);
                            }
                        } else {
                            return Err(QueryError::Unsupported);
                        }
                    } else {
                        return Err(QueryError::Unsupported);
                    }
                } else {
                    return Err(QueryError::Unsupported);
                }
            } else {
                return Err(QueryError::Unsupported);
            }
        } else {
            return Err(QueryError::Unsupported);
        };
        // Find new value assignment.
        let mut new_val = None;
        for assign in assignments {
            if let AssignmentTarget::ColumnName(name) = assign.target {
                if let Some(id) = name.0.first().and_then(|p| p.as_ident()) {
                    if id.value.to_lowercase() == "value" {
                        if let Expr::Value(v) = assign.value {
                            if let Value::SingleQuotedString(s) = v.value {
                                new_val = Some(s);
                            }
                        }
                    }
                }
            }
        }
        let val = new_val.ok_or(QueryError::Unsupported)?;
        db.insert_ns(&ns, key, val.into_bytes()).await;
        Ok(())
    }

    /// Handle a `DELETE` statement for a single key.
    async fn exec_delete(&self, db: &Database, delete: Delete) -> Result<(), QueryError> {
        let table = match &delete.from {
            FromTable::WithFromKeyword(t) | FromTable::WithoutKeyword(t) => t,
        };
        if table.len() != 1 {
            return Err(QueryError::Unsupported);
        }
        let ns = table_factor_to_ns(&table[0].relation).ok_or(QueryError::Unsupported)?;
        register_table(db, &ns).await;
        // Only support deletion by exact key equality.
        if let Some(expr) = delete.selection {
            if let Expr::BinaryOp { left, op, right } = expr {
                if op == BinaryOperator::Eq {
                    if let (Expr::Identifier(id), Expr::Value(v)) = (*left, *right) {
                        if id.value.to_lowercase() == "key" {
                            if let Value::SingleQuotedString(s) = v.value {
                                db.delete_ns(&ns, &s).await;
                            }
                        }
                    }
                }
            }
        }
        Ok(())
    }

    /// Execute the inner query of a [`Statement::Query`].
    async fn exec_query(
        &self,
        db: &Database,
        q: Box<Query>,
    ) -> Result<Option<Vec<u8>>, QueryError> {
        match *q.body {
            SetExpr::Select(select) => {
                self.exec_select(db, *select, q.order_by, q.limit_clause)
                    .await
            }
            _ => Err(QueryError::Unsupported),
        }
    }

    /// Execute a `SELECT` statement returning at most one row/value.
    async fn exec_select(
        &self,
        db: &Database,
        select: Select,
        order: Option<OrderBy>,
        limit: Option<LimitClause>,
    ) -> Result<Option<Vec<u8>>, QueryError> {
        if select.from.len() != 1 {
            return Err(QueryError::Unsupported);
        }
        let ns = table_factor_to_ns(&select.from[0].relation).ok_or(QueryError::Unsupported)?;
        register_table(db, &ns).await;
        let mut rows = db.scan_ns(&ns).await;
        if let Some(cond) = select.selection {
            rows.retain(|(k, v)| eval_cond(&cond, k, v));
        }

        if let sqlparser::ast::GroupByExpr::Expressions(exprs, _) = &select.group_by {
            if !exprs.is_empty() {
                return self.exec_group_select(exprs, &select.projection, rows, limit);
            }
        }

        // Apply DISTINCT by sorting and removing duplicates.
        if select.distinct.is_some() {
            rows.sort_by(|a, b| a.cmp(b));
            rows.dedup();
        }

        // Handle simple ORDER BY on a single column.
        if let Some(order) = order {
            if let OrderByKind::Expressions(exprs) = order.kind {
                if let Some(ob) = exprs.first() {
                    let asc = ob.options.asc.unwrap_or(true);
                    if let Expr::Identifier(id) = &ob.expr {
                        let col = id.value.to_lowercase();
                        rows.sort_by(|a, b| {
                            let av = column_value(&col, &a.0, &a.1);
                            let bv = column_value(&col, &b.0, &b.1);
                            if asc { av.cmp(&bv) } else { bv.cmp(&av) }
                        });
                    }
                }
            }
        }

        if let Some(lc) = limit {
            apply_limit_rows(&mut rows, &lc);
        }

        if select.projection.len() != 1 {
            return Err(QueryError::Unsupported);
        }
        let item = &select.projection[0];
        let result = match item {
            SelectItem::Wildcard(_) => rows.first().map(|(_, v)| v.clone()),
            SelectItem::UnnamedExpr(Expr::Function(func)) => Some(handle_function(func, &rows)?),
            SelectItem::UnnamedExpr(expr) => {
                let mut out: Vec<String> = rows
                    .iter()
                    .filter_map(|(k, v)| eval_expr(expr, k, v))
                    .collect();
                if select.distinct.is_some() {
                    out.sort();
                    out.dedup();
                }
                out.first().map(|s| s.clone().into_bytes())
            }
            _ => return Err(QueryError::Unsupported),
        };
        Ok(result)
    }

    /// Execute a `SELECT` with a single `GROUP BY` expression.
    fn exec_group_select(
        &self,
        group_exprs: &Vec<Expr>,
        projection: &Vec<SelectItem>,
        rows: Vec<(String, Vec<u8>)>,
        limit: Option<LimitClause>,
    ) -> Result<Option<Vec<u8>>, QueryError> {
        if group_exprs.len() != 1 || projection.len() != 1 {
            return Err(QueryError::Unsupported);
        }
        let group_expr = &group_exprs[0];
        let mut groups: BTreeMap<String, Vec<(String, Vec<u8>)>> = BTreeMap::new();
        for (k, v) in rows {
            if let Some(key) = eval_expr(group_expr, &k, &v) {
                groups.entry(key).or_default().push((k, v));
            }
        }
        let item = &projection[0];
        let mut out: Vec<Vec<u8>> = Vec::new();
        for (gk, grp_rows) in groups {
            let val = match item {
                SelectItem::Wildcard(_) => gk.into_bytes(),
                SelectItem::UnnamedExpr(Expr::Function(func)) => handle_function(func, &grp_rows)?,
                SelectItem::UnnamedExpr(expr) => eval_expr(expr, &grp_rows[0].0, &grp_rows[0].1)
                    .unwrap_or_default()
                    .into_bytes(),
                _ => return Err(QueryError::Unsupported),
            };
            out.push(val);
        }
        if let Some(lc) = limit {
            apply_limit_vec(&mut out, &lc);
        }
        Ok(out.into_iter().next())
    }

    /// Return a newline-delimited list of registered table names.
    async fn exec_show_tables(&self, db: &Database) -> Result<Option<Vec<u8>>, QueryError> {
        let mut tables: Vec<String> = db
            .scan_ns("_tables")
            .await
            .into_iter()
            .map(|(k, _)| k)
            .collect();
        tables.sort();
        Ok(Some(tables.join("\n").into_bytes()))
    }
}

/// Register a table name in the internal catalog if it does not already exist.
async fn register_table(db: &Database, table: &str) {
    if db.get_ns("_tables", table).await.is_none() {
        db.insert_ns("_tables", table.to_string(), Vec::new()).await;
    }
}

/// Convert an AST [`ObjectName`] into a lowercase namespace string.
fn object_name_to_ns(name: &ObjectName) -> Option<String> {
    name.0
        .last()
        .and_then(|p| p.as_ident())
        .map(|i| i.value.to_lowercase())
}

/// Extract a namespace from a [`TableFactor`].
fn table_factor_to_ns(tf: &TableFactor) -> Option<String> {
    match tf {
        TableFactor::Table { name, .. } => object_name_to_ns(name),
        _ => None,
    }
}

/// Retrieve the requested column value from a key/value pair.
fn column_value(col: &str, key: &str, val: &[u8]) -> String {
    if col == "key" {
        key.to_string()
    } else {
        String::from_utf8_lossy(val).to_string()
    }
}

/// Evaluate a simple expression against a row, returning the result as a string.
fn eval_expr(expr: &Expr, key: &str, val: &[u8]) -> Option<String> {
    match expr {
        Expr::Identifier(id) => Some(column_value(&id.value.to_lowercase(), key, val)),
        Expr::Value(v) => match &v.value {
            Value::SingleQuotedString(s) => Some(s.clone()),
            Value::Number(n, _) => Some(n.clone()),
            _ => None,
        },
        Expr::Cast {
            expr, data_type, ..
        } => {
            let inner = eval_expr(expr, key, val)?;
            match data_type {
                DataType::Int(_)
                | DataType::Integer(_)
                | DataType::BigInt(_)
                | DataType::SmallInt(_)
                | DataType::Unsigned => inner.parse::<i64>().ok().map(|i| i.to_string()),
                DataType::Text | DataType::Varchar(_) | DataType::Char(_) => Some(inner),
                _ => None,
            }
        }
        _ => None,
    }
}

/// Evaluate a boolean condition against a key/value pair.
fn eval_cond(expr: &Expr, key: &str, val: &[u8]) -> bool {
    match expr {
        Expr::BinaryOp { left, op, right } => {
            if let (Some(lhs), Some(rhs)) = (eval_expr(left, key, val), eval_expr(right, key, val))
            {
                match op {
                    BinaryOperator::Eq => lhs == rhs,
                    BinaryOperator::NotEq => lhs != rhs,
                    BinaryOperator::Lt => lhs < rhs,
                    BinaryOperator::Gt => lhs > rhs,
                    BinaryOperator::LtEq => lhs <= rhs,
                    BinaryOperator::GtEq => lhs >= rhs,
                    BinaryOperator::Custom(op) if op.eq_ignore_ascii_case("contains") => {
                        lhs.contains(&rhs)
                    }
                    _ => false,
                }
            } else {
                false
            }
        }
        Expr::InList {
            expr,
            list,
            negated,
        } => {
            if let Some(target) = eval_expr(expr, key, val) {
                let found = list
                    .iter()
                    .any(|e| eval_expr(e, key, val).map_or(false, |s| s == target));
                if *negated { !found } else { found }
            } else {
                false
            }
        }
        _ => false,
    }
}

/// Execute a simple aggregate function over the provided `rows`.
fn handle_function(func: &Function, rows: &[(String, Vec<u8>)]) -> Result<Vec<u8>, QueryError> {
    let name = func.name.to_string().to_lowercase();
    match name.as_str() {
        "count" => Ok(rows.len().to_string().into_bytes()),
        "min" | "max" => {
            let expr = extract_func_expr(func)?;
            let mut vals: Vec<String> = rows
                .iter()
                .filter_map(|(k, v)| eval_expr(expr, k, v))
                .collect();
            vals.sort();
            if name == "min" {
                Ok(vals.first().unwrap_or(&"".to_string()).clone().into_bytes())
            } else {
                Ok(vals.last().unwrap_or(&"".to_string()).clone().into_bytes())
            }
        }
        "sum" => {
            let expr = extract_func_expr(func)?;
            let sum: i64 = rows
                .iter()
                .filter_map(|(k, v)| eval_expr(expr, k, v)?.parse::<i64>().ok())
                .sum();
            Ok(sum.to_string().into_bytes())
        }
        _ => Err(QueryError::Unsupported),
    }
}

/// Return the first argument expression from a function call.
fn extract_func_expr(func: &Function) -> Result<&Expr, QueryError> {
    if let FunctionArguments::List(list) = &func.args {
        if let Some(FunctionArg::Unnamed(FunctionArgExpr::Expr(expr))) = list.args.first() {
            Ok(expr)
        } else {
            Err(QueryError::Unsupported)
        }
    } else {
        Err(QueryError::Unsupported)
    }
}

/// Extract a numeric limit value from a [`LimitClause`].
fn extract_limit(lc: &LimitClause) -> Option<usize> {
    match lc {
        LimitClause::LimitOffset {
            limit: Some(Expr::Value(v)),
            ..
        }
        | LimitClause::OffsetCommaLimit {
            limit: Expr::Value(v),
            ..
        } => {
            if let Value::Number(n, _) = &v.value {
                n.parse::<usize>().ok()
            } else {
                None
            }
        }
        _ => None,
    }
}

/// Apply a LIMIT clause to a vector of row tuples.
fn apply_limit_rows(rows: &mut Vec<(String, Vec<u8>)>, lc: &LimitClause) {
    if let Some(l) = extract_limit(lc) {
        if rows.len() > l {
            rows.truncate(l);
        }
    }
}

/// Apply a LIMIT clause to a generic vector.
fn apply_limit_vec<T>(v: &mut Vec<T>, lc: &LimitClause) {
    if let Some(l) = extract_limit(lc) {
        if v.len() > l {
            v.truncate(l);
        }
    }
}
