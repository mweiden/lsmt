//! Minimal SQL execution engine for the key-value store.

use std::collections::BTreeMap;

use sqlparser::ast::{
    Assignment, AssignmentTarget, BinaryOperator, ColumnOption, DataType, Delete, Expr, FromTable,
    Insert, LimitClause, ObjectName, ObjectType, OrderBy, Query, Select, SelectItem, SetExpr,
    Statement, TableFactor, TableWithJoins, Value,
};
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;

use crate::{
    Database,
    schema::{TableSchema, decode_row, encode_row},
};

/// Errors that can occur when parsing or executing a query.
#[derive(thiserror::Error, Debug)]
pub enum QueryError {
    /// The input SQL could not be parsed.
    #[error("parse: {0}")]
    Parse(#[from] sqlparser::parser::ParserError),
    /// The query is syntactically valid but not supported by the engine.
    #[error("unsupported query")]
    Unsupported,
    /// Any other internal or I/O error.
    #[error("{0}")]
    Other(String),
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
                if db.get_ns("_tables", &ns).await.is_some() {
                    return Err(QueryError::Unsupported);
                }
                let schema = schema_from_create(&ct).ok_or(QueryError::Unsupported)?;
                save_schema(db, &ns, &schema).await;
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
                    db.delete_ns("_schemas", &ns).await;
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
        let ns = match &insert.table {
            sqlparser::ast::TableObject::TableName(name) => {
                object_name_to_ns(name).ok_or(QueryError::Unsupported)?
            }
            _ => return Err(QueryError::Unsupported),
        };
        let schema = get_schema(db, &ns).await.ok_or(QueryError::Unsupported)?;
        let source = insert.source.ok_or(QueryError::Unsupported)?;
        let values = match *source.body {
            SetExpr::Values(v) => v,
            _ => return Err(QueryError::Unsupported),
        };
        let row = values.rows.get(0).ok_or(QueryError::Unsupported)?;

        // Determine column order for the insert.
        let cols: Vec<String> = if !insert.columns.is_empty() {
            insert
                .columns
                .iter()
                .map(|c| c.value.to_lowercase())
                .collect()
        } else {
            schema.columns.clone()
        };
        if cols.len() != row.len() {
            return Err(QueryError::Unsupported);
        }

        // Build key and value map for schema tables.
        let mut key_parts: Vec<String> = Vec::new();
        let mut data_map: BTreeMap<String, String> = BTreeMap::new();
        for (col, expr) in cols.iter().zip(row.iter()) {
            let val = expr_to_string(expr).ok_or(QueryError::Unsupported)?;
            if schema.partition_keys.contains(col) || schema.clustering_keys.contains(col) {
                key_parts.push(val.clone());
            } else {
                data_map.insert(col.clone(), val);
            }
        }
        let key = key_parts.join("|");
        let data = encode_row(&data_map);
        db.insert_ns(&ns, key, data).await;
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
        let schema = get_schema(db, &ns).await.ok_or(QueryError::Unsupported)?;
        // schema-aware path
        let cond = selection.ok_or(QueryError::Unsupported)?;
        let cond_map = where_to_map(&cond);
        let key_cols = schema.key_columns();
        let mut key_parts = Vec::new();
        for col in key_cols {
            if let Some(v) = cond_map.get(&col) {
                key_parts.push(v.clone());
            } else {
                return Err(QueryError::Unsupported);
            }
        }
        let key = key_parts.join("|");
        let mut row_map = if let Some(bytes) = db.get_ns(&ns, &key).await {
            decode_row(&bytes)
        } else {
            BTreeMap::new()
        };
        for assign in assignments {
            if let AssignmentTarget::ColumnName(name) = assign.target {
                if let Some(id) = name.0.first().and_then(|p| p.as_ident()) {
                    let col = id.value.to_lowercase();
                    if schema.partition_keys.contains(&col) || schema.clustering_keys.contains(&col)
                    {
                        continue; // skip key columns
                    }
                    let val = expr_to_string(&assign.value).ok_or(QueryError::Unsupported)?;
                    row_map.insert(col, val);
                }
            }
        }
        let data = encode_row(&row_map);
        db.insert_ns(&ns, key, data).await;
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
        let schema = get_schema(db, &ns).await.ok_or(QueryError::Unsupported)?;
        // schema-aware deletion using key columns
        let expr = delete.selection.ok_or(QueryError::Unsupported)?;
        let cond_map = where_to_map(&expr);
        let key_cols = schema.key_columns();
        let mut key_parts = Vec::new();
        for col in key_cols {
            if let Some(v) = cond_map.get(&col) {
                key_parts.push(v.clone());
            } else {
                return Err(QueryError::Unsupported);
            }
        }
        let key = key_parts.join("|");
        db.delete_ns(&ns, &key).await;
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
        _order: Option<OrderBy>,
        _limit: Option<LimitClause>,
    ) -> Result<Option<Vec<u8>>, QueryError> {
        if select.from.len() != 1 {
            return Err(QueryError::Unsupported);
        }
        let ns = table_factor_to_ns(&select.from[0].relation).ok_or(QueryError::Unsupported)?;
        let schema = get_schema(db, &ns).await.ok_or(QueryError::Unsupported)?;

        // handle COUNT(*) with optional WHERE filtering
        if select.projection.len() == 1 {
            if let SelectItem::UnnamedExpr(Expr::Function(func)) = &select.projection[0] {
                if func.name.to_string().to_lowercase() == "count" {
                    let selection = select.selection.clone();
                    return self.exec_count(db, &ns, &schema, selection).await;
                }
            }
        }

        self.exec_select_schema(db, &ns, &schema, select).await
    }

    /// Execute a `COUNT(*)` query with optional equality filters.
    async fn exec_count(
        &self,
        db: &Database,
        ns: &str,
        schema: &TableSchema,
        selection: Option<Expr>,
    ) -> Result<Option<Vec<u8>>, QueryError> {
        let cond_map = if let Some(expr) = selection {
            where_to_map(&expr)
        } else {
            BTreeMap::new()
        };
        let key_cols = schema.key_columns();
        // If all key columns are present, we can directly look up the row.
        if key_cols.iter().all(|c| cond_map.contains_key(c)) {
            let key_parts: Vec<String> = key_cols
                .iter()
                .map(|c| cond_map.get(c).cloned().unwrap())
                .collect();
            let key = key_parts.join("|");
            if let Some(bytes) = db.get_ns(ns, &key).await {
                let mut row_map = decode_row(&bytes);
                for col in &key_cols {
                    if let Some(v) = cond_map.get(col) {
                        row_map.insert(col.clone(), v.clone());
                    }
                }
                for (col, val) in &cond_map {
                    if row_map.get(col).map_or(true, |v| v != val) {
                        return Ok(Some("0".into()));
                    }
                }
                return Ok(Some("1".into()));
            } else {
                return Ok(Some("0".into()));
            }
        }
        // Otherwise scan the namespace and filter.
        let rows = db.scan_ns(ns).await;
        let mut count = 0;
        for (k, bytes) in rows {
            let mut row_map = decode_row(&bytes);
            for (col, part) in key_cols.iter().zip(k.split('|')) {
                row_map.insert(col.clone(), part.to_string());
            }
            let mut matches = true;
            for (col, val) in &cond_map {
                if row_map.get(col).map_or(true, |v| v != val) {
                    matches = false;
                    break;
                }
            }
            if matches {
                count += 1;
            }
        }
        Ok(Some(count.to_string().into_bytes()))
    }

    /// Simplified `SELECT` handler for schema-aware tables.
    async fn exec_select_schema(
        &self,
        db: &Database,
        ns: &str,
        schema: &TableSchema,
        select: Select,
    ) -> Result<Option<Vec<u8>>, QueryError> {
        if select.projection.len() != 1 {
            return Err(QueryError::Unsupported);
        }
        // Extract key columns from WHERE clause.
        let cond_map = if let Some(expr) = select.selection {
            where_to_map(&expr)
        } else {
            BTreeMap::new()
        };
        let key_cols = schema.key_columns();
        let mut key_parts: Vec<String> = Vec::new();
        for col in key_cols {
            if let Some(v) = cond_map.get(&col) {
                key_parts.push(v.clone());
            } else {
                return Err(QueryError::Unsupported);
            }
        }
        let key = key_parts.join("|");
        let row_bytes = db.get_ns(ns, &key).await;
        let row_bytes = if let Some(b) = row_bytes {
            b
        } else {
            return Ok(None);
        };
        let mut row_map = decode_row(&row_bytes);
        // include key columns
        for (col, val) in cond_map {
            row_map.insert(col, val);
        }

        let item = &select.projection[0];
        let result = match item {
            SelectItem::UnnamedExpr(Expr::Identifier(id)) => row_map
                .get(&id.value.to_lowercase())
                .cloned()
                .map(|s| s.into_bytes()),
            SelectItem::UnnamedExpr(Expr::Cast {
                expr, data_type, ..
            }) => {
                if let Expr::Identifier(id) = &**expr {
                    if let Some(val) = row_map.get(&id.value.to_lowercase()) {
                        cast_simple(val, data_type).map(|s| s.into_bytes())
                    } else {
                        None
                    }
                } else {
                    None
                }
            }
            SelectItem::Wildcard(_) => Some(encode_row(&row_map)),
            _ => return Err(QueryError::Unsupported),
        };
        Ok(result)
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

    /// Extract partition key values from `sql` for routing and replication.
    pub async fn partition_keys(
        &self,
        db: &Database,
        sql: &str,
    ) -> Result<Vec<String>, QueryError> {
        let stmts = self.parse(sql)?;
        let mut keys = Vec::new();
        for stmt in stmts {
            match stmt {
                Statement::Insert(insert) => {
                    let ns = match &insert.table {
                        sqlparser::ast::TableObject::TableName(name) => {
                            object_name_to_ns(name).ok_or(QueryError::Unsupported)?
                        }
                        _ => return Err(QueryError::Unsupported),
                    };
                    let schema = get_schema(db, &ns).await.ok_or(QueryError::Unsupported)?;
                    let source = insert.source.ok_or(QueryError::Unsupported)?;
                    let values = match *source.body {
                        SetExpr::Values(v) => v,
                        _ => return Err(QueryError::Unsupported),
                    };
                    let row = values.rows.get(0).ok_or(QueryError::Unsupported)?;
                    let cols: Vec<String> = if !insert.columns.is_empty() {
                        insert
                            .columns
                            .iter()
                            .map(|c| c.value.to_lowercase())
                            .collect()
                    } else {
                        schema.columns.clone()
                    };
                    let mut parts = Vec::new();
                    for (col, expr) in cols.iter().zip(row.iter()) {
                        if schema.partition_keys.contains(col) {
                            let val = expr_to_string(expr).ok_or(QueryError::Unsupported)?;
                            parts.push(val);
                        }
                    }
                    if !parts.is_empty() {
                        keys.push(parts.join("|"));
                    }
                }
                Statement::Update {
                    table, selection, ..
                } => {
                    let ns = table_factor_to_ns(&table.relation).ok_or(QueryError::Unsupported)?;
                    let schema = get_schema(db, &ns).await.ok_or(QueryError::Unsupported)?;
                    if let Some(expr) = selection {
                        let map = where_to_multi_map(&expr);
                        keys.extend(build_keys(&schema.partition_keys, &map));
                    }
                }
                Statement::Delete(delete) => {
                    let table = match &delete.from {
                        FromTable::WithFromKeyword(t) | FromTable::WithoutKeyword(t) => t,
                    };
                    if table.len() != 1 {
                        return Err(QueryError::Unsupported);
                    }
                    let ns =
                        table_factor_to_ns(&table[0].relation).ok_or(QueryError::Unsupported)?;
                    let schema = get_schema(db, &ns).await.ok_or(QueryError::Unsupported)?;
                    if let Some(expr) = delete.selection {
                        let map = where_to_multi_map(&expr);
                        keys.extend(build_keys(&schema.partition_keys, &map));
                    }
                }
                Statement::Query(q) => {
                    if let SetExpr::Select(select) = *q.body {
                        if select.from.len() != 1 {
                            return Err(QueryError::Unsupported);
                        }
                        let ns = table_factor_to_ns(&select.from[0].relation)
                            .ok_or(QueryError::Unsupported)?;
                        let schema = get_schema(db, &ns).await.ok_or(QueryError::Unsupported)?;
                        if let Some(expr) = select.selection {
                            let map = where_to_multi_map(&expr);
                            keys.extend(build_keys(&schema.partition_keys, &map));
                        }
                    }
                }
                _ => {}
            }
        }
        Ok(keys)
    }
}

/// Register a table name in the internal catalog if it does not already exist.
async fn register_table(db: &Database, table: &str) {
    if db.get_ns("_tables", table).await.is_none() {
        db.insert_ns("_tables", table.to_string(), Vec::new()).await;
    }
}

/// Retrieve the schema for a table if it exists.
async fn get_schema(db: &Database, table: &str) -> Option<TableSchema> {
    db.get_ns("_schemas", table)
        .await
        .and_then(|v| serde_json::from_slice(&v).ok())
}

/// Persist a schema definition for a table.
async fn save_schema(db: &Database, table: &str, schema: &TableSchema) {
    let data = serde_json::to_vec(schema).unwrap_or_default();
    db.insert_ns("_schemas", table.to_string(), data).await;
}

/// Build a [`TableSchema`] from a parsed `CREATE TABLE` statement.
fn schema_from_create(ct: &sqlparser::ast::CreateTable) -> Option<TableSchema> {
    let columns: Vec<String> = ct
        .columns
        .iter()
        .map(|c| c.name.value.to_lowercase())
        .collect();
    if columns.is_empty() {
        return None;
    }
    // Determine primary key columns.
    let mut pk: Vec<String> = Vec::new();
    let mut ck: Vec<String> = Vec::new();
    for constr in &ct.constraints {
        if let sqlparser::ast::TableConstraint::PrimaryKey { columns, .. } = constr {
            for (i, ic) in columns.iter().enumerate() {
                if let Expr::Identifier(id) = &ic.column.expr {
                    if i == 0 {
                        pk.push(id.value.to_lowercase());
                    } else {
                        ck.push(id.value.to_lowercase());
                    }
                }
            }
        }
    }
    // If no table constraint primary key, look for column options.
    if pk.is_empty() {
        for col in &ct.columns {
            for opt in &col.options {
                if let ColumnOption::Unique {
                    is_primary: true, ..
                } = opt.option
                {
                    pk.push(col.name.value.to_lowercase());
                }
            }
        }
    }
    if pk.is_empty() {
        return None;
    }
    Some(TableSchema::new(pk, ck, columns))
}

/// Convert a simple expression into a string value.
fn expr_to_string(expr: &Expr) -> Option<String> {
    match expr {
        Expr::Value(v) => match &v.value {
            Value::SingleQuotedString(s) => Some(s.clone()),
            Value::Number(n, _) => Some(n.clone()),
            _ => None,
        },
        _ => None,
    }
}

/// Convert a WHERE clause into a map of column -> possible values, supporting `IN` lists.
fn where_to_multi_map(expr: &Expr) -> BTreeMap<String, Vec<String>> {
    fn collect(e: &Expr, out: &mut BTreeMap<String, Vec<String>>) {
        match e {
            Expr::BinaryOp { left, op, right } => {
                if *op == BinaryOperator::And {
                    collect(left, out);
                    collect(right, out);
                } else if *op == BinaryOperator::Eq {
                    if let Expr::Identifier(id) = &**left {
                        if let Some(val) = expr_to_string(right) {
                            out.entry(id.value.to_lowercase()).or_default().push(val);
                        }
                    }
                }
            }
            Expr::InList { expr, list, .. } => {
                if let Expr::Identifier(id) = &**expr {
                    let vals: Vec<String> = list.iter().filter_map(expr_to_string).collect();
                    if !vals.is_empty() {
                        out.entry(id.value.to_lowercase()).or_default().extend(vals);
                    }
                }
            }
            _ => {}
        }
    }
    let mut map = BTreeMap::new();
    collect(expr, &mut map);
    map
}

/// Build partition key strings from column values, generating all combinations.
fn build_keys(pk_cols: &[String], map: &BTreeMap<String, Vec<String>>) -> Vec<String> {
    if pk_cols.is_empty() {
        return Vec::new();
    }
    let mut lists = Vec::new();
    for col in pk_cols {
        if let Some(vals) = map.get(col) {
            lists.push(vals.clone());
        } else {
            return Vec::new();
        }
    }
    fn expand(idx: usize, lists: &[Vec<String>], cur: &mut Vec<String>, out: &mut Vec<String>) {
        if idx == lists.len() {
            out.push(cur.join("|"));
            return;
        }
        for v in &lists[idx] {
            cur.push(v.clone());
            expand(idx + 1, lists, cur, out);
            cur.pop();
        }
    }
    let mut out = Vec::new();
    expand(0, &lists, &mut Vec::new(), &mut out);
    out
}

/// Convert a simple WHERE clause into a map of column -> value.
fn where_to_map(expr: &Expr) -> BTreeMap<String, String> {
    fn collect(e: &Expr, out: &mut BTreeMap<String, String>) {
        match e {
            Expr::BinaryOp { left, op, right } => {
                if *op == BinaryOperator::And {
                    collect(left, out);
                    collect(right, out);
                } else if *op == BinaryOperator::Eq {
                    if let Expr::Identifier(id) = &**left {
                        if let Some(val) = expr_to_string(right) {
                            out.insert(id.value.to_lowercase(), val);
                        }
                    }
                }
            }
            _ => {}
        }
    }
    let mut map = BTreeMap::new();
    collect(expr, &mut map);
    map
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

/// Perform a basic cast of `val` to the specified [`DataType`].
fn cast_simple(val: &str, data_type: &DataType) -> Option<String> {
    match data_type {
        DataType::Int(_)
        | DataType::Integer(_)
        | DataType::BigInt(_)
        | DataType::SmallInt(_)
        | DataType::Unsigned => val.parse::<i64>().ok().map(|i| i.to_string()),
        DataType::Text | DataType::Varchar(_) | DataType::Char(_) => Some(val.to_string()),
        _ => None,
    }
}
