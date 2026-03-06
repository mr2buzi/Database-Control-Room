use std::path::Path;
use std::time::Instant;

use crate::ast::{
    CreateIndexStatement, CreateTableStatement, DeleteStatement, InsertStatement, Projection,
    SelectStatement, Statement,
};
use crate::catalog::{Catalog, IndexMeta, TableMeta};
use crate::common::{escape_json, ColumnType, Error, Result, Value};
use crate::index::btree::{build_index_pages, search_index};
use crate::parser::{parse_statement, parse_statements};
use crate::planner::{build_plan, Plan};
use crate::storage::pager::Pager;
use crate::storage::table::{allocate_heap_page, column_index, fetch_row, insert_row, mark_deleted, scan_rows};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OutputFormat {
    Table,
    Json,
}

#[derive(Debug, Clone)]
pub struct ExecutionOutput {
    pub rendered: String,
}

#[derive(Debug, Clone)]
struct QueryStats {
    latency_ms: f64,
    rows_read: usize,
    rows_returned: usize,
    used_index: bool,
}

#[derive(Debug, Clone)]
struct ExecutionEnvelope {
    ast: Statement,
    plan: Plan,
    columns: Vec<String>,
    rows: Vec<Vec<Value>>,
    stats: QueryStats,
    message: Option<String>,
}

#[derive(Debug, Clone)]
pub struct SchemaColumn {
    pub name: String,
    pub column_type: ColumnType,
    pub indexed: bool,
}

#[derive(Debug, Clone)]
pub struct SchemaTable {
    pub name: String,
    pub row_count: usize,
    pub columns: Vec<SchemaColumn>,
}

pub struct Database {
    pager: Pager,
    catalog: Catalog,
    in_transaction: bool,
}

impl Database {
    pub fn open(path: &Path) -> Result<Self> {
        let mut pager = Pager::open(path)?;
        let catalog = Catalog::deserialize(&pager.read_catalog()?)?;
        Ok(Self {
            pager,
            catalog,
            in_transaction: false,
        })
    }

    pub fn execute_statement(
        &mut self,
        statement: Statement,
        format: OutputFormat,
        _debug_ast: bool,
    ) -> Result<ExecutionOutput> {
        let envelope = self.execute_statement_envelope(statement)?;

        Ok(ExecutionOutput {
            rendered: match format {
                OutputFormat::Table => render_table_output(&envelope),
                OutputFormat::Json => render_json_output(&envelope),
            },
        })
    }

    pub fn execute_query_text(
        &mut self,
        query: &str,
        format: OutputFormat,
    ) -> Result<ExecutionOutput> {
        let statements = parse_statements(query)?;
        let mut envelopes = Vec::with_capacity(statements.len());
        for statement in statements {
            envelopes.push(self.execute_statement_envelope(statement)?);
        }

        Ok(ExecutionOutput {
            rendered: match format {
                OutputFormat::Table => envelopes
                    .iter()
                    .map(render_table_output)
                    .collect::<Vec<_>>()
                    .join("\n\n"),
                OutputFormat::Json => render_json_batch_output(&envelopes),
            },
        })
    }

    pub fn inspect_schema(&mut self) -> Result<Vec<SchemaTable>> {
        let mut tables = Vec::new();
        for table in &self.catalog.tables.clone() {
            let row_count = scan_rows(&mut self.pager, table)?.len();
            let indexes = self.catalog.indexes_for_table(table.id);
            tables.push(SchemaTable {
                name: table.name.clone(),
                row_count,
                columns: table
                    .columns
                    .iter()
                    .map(|column| SchemaColumn {
                        name: column.name.clone(),
                        column_type: column.column_type,
                        indexed: indexes
                            .iter()
                            .any(|index| index.column_name.eq_ignore_ascii_case(&column.name)),
                    })
                    .collect(),
            });
        }
        Ok(tables)
    }

    pub fn inspect_schema_json(&mut self) -> Result<String> {
        let tables = self.inspect_schema()?;
        Ok(format!(
            "{{\"tables\":[{}],\"error\":null}}",
            tables
                .iter()
                .map(|table| format!(
                    "{{\"name\":\"{}\",\"rowCount\":{},\"columns\":[{}]}}",
                    escape_json(&table.name),
                    table.row_count,
                    table
                        .columns
                        .iter()
                        .map(|column| format!(
                            "{{\"name\":\"{}\",\"type\":\"{}\",\"indexed\":{}}}",
                            escape_json(&column.name),
                            column.column_type.as_str(),
                            if column.indexed { "true" } else { "false" }
                        ))
                        .collect::<Vec<_>>()
                        .join(",")
                ))
                .collect::<Vec<_>>()
                .join(",")
        ))
    }

    fn execute_statement_envelope(&mut self, statement: Statement) -> Result<ExecutionEnvelope> {
        let plan = build_plan(&statement, &self.catalog)?;
        let envelope = match &statement {
            Statement::MetaCommand(_) => Ok(ExecutionEnvelope {
                ast: statement,
                plan,
                columns: Vec::new(),
                rows: Vec::new(),
                stats: QueryStats {
                    latency_ms: 0.0,
                    rows_read: 0,
                    rows_returned: 0,
                    used_index: false,
                },
                message: None,
            }),
            Statement::Begin => {
                if self.in_transaction {
                    return Err(Error::Transaction("transaction already active".into()));
                }
                self.in_transaction = true;
                Ok(self.simple_response(statement, plan, "transaction started"))
            }
            Statement::Commit => {
                if !self.in_transaction {
                    return Err(Error::Transaction("no active transaction".into()));
                }
                self.pager.commit()?;
                self.catalog = Catalog::deserialize(&self.pager.read_catalog()?)?;
                self.in_transaction = false;
                Ok(self.simple_response(statement, plan, "transaction committed"))
            }
            Statement::Rollback => {
                if !self.in_transaction {
                    return Err(Error::Transaction("no active transaction".into()));
                }
                self.pager.rollback()?;
                self.catalog = Catalog::deserialize(&self.pager.read_catalog()?)?;
                self.in_transaction = false;
                Ok(self.simple_response(statement, plan, "transaction rolled back"))
            }
            Statement::Explain(select) => {
                let started = Instant::now();
                let plan_text = plan.to_text();
                let used_index = plan.used_index.is_some();
                let stats = QueryStats {
                    latency_ms: started.elapsed().as_secs_f64() * 1000.0,
                    rows_read: 0,
                    rows_returned: 0,
                    used_index,
                };
                Ok(ExecutionEnvelope {
                    ast: Statement::Explain(select.clone()),
                    plan,
                    columns: vec!["plan".into()],
                    rows: vec![vec![Value::Text(plan_text)]],
                    stats,
                    message: Some("explain plan".into()),
                })
            }
            Statement::Select(select) => {
                self.execute_select(statement.clone(), select.clone(), plan)
            }
            Statement::CreateTable(create) => {
                self.run_mutation(statement.clone(), plan, |database| database.create_table(create))
            }
            Statement::Insert(insert) => {
                self.run_mutation(statement.clone(), plan, |database| database.insert(insert))
            }
            Statement::Delete(delete) => {
                self.run_mutation(statement.clone(), plan, |database| database.delete(delete))
            }
            Statement::CreateIndex(create_index) => {
                self.run_mutation(statement.clone(), plan, |database| {
                    database.create_index(create_index)
                })
            }
        }?;
        Ok(envelope)
    }

    fn simple_response(&self, ast: Statement, plan: Plan, message: &str) -> ExecutionEnvelope {
        ExecutionEnvelope {
            ast,
            plan,
            columns: Vec::new(),
            rows: Vec::new(),
            stats: QueryStats {
                latency_ms: 0.0,
                rows_read: 0,
                rows_returned: 0,
                used_index: false,
            },
            message: Some(message.into()),
        }
    }

    fn execute_select(
        &mut self,
        ast: Statement,
        select: SelectStatement,
        plan: Plan,
    ) -> Result<ExecutionEnvelope> {
        let started = Instant::now();
        let table = self
            .catalog
            .table(&select.table_name)
            .ok_or_else(|| Error::Catalog(format!("table {} not found", select.table_name)))?
            .clone();

        let projection_indices = projection_indices(&table, &select.projection)?;
        let columns = projection_indices
            .iter()
            .map(|index| table.columns[*index].name.clone())
            .collect::<Vec<_>>();

        let (rows_read, rows) = if let Some(filter) = &select.filter {
            if let Some(index_meta) = self.catalog.index_for_column(table.id, &filter.column) {
                if index_meta.key_type == filter.value.value_type() {
                    let row_ids = search_index(&mut self.pager, index_meta.root_page_id, &filter.value)?
                        .row_ids;
                    let mut materialized = Vec::new();
                    for row_id in &row_ids {
                        if let Some(row) = fetch_row(&mut self.pager, &table, *row_id)? {
                            if value_matches_filter(&row.values, &table, filter)? {
                                materialized.push(project_values(&row.values, &projection_indices));
                                if select.limit.is_some_and(|limit| materialized.len() >= limit) {
                                    break;
                                }
                            }
                        }
                    }
                    (
                        select
                            .limit
                            .map(|limit| row_ids.len().min(limit))
                            .unwrap_or(row_ids.len()),
                        materialized,
                    )
                } else {
                    scan_select_rows(
                        &mut self.pager,
                        &table,
                        &projection_indices,
                        select.filter.as_ref(),
                        select.limit,
                    )?
                }
            } else {
                scan_select_rows(
                    &mut self.pager,
                    &table,
                    &projection_indices,
                    select.filter.as_ref(),
                    select.limit,
                )?
            }
        } else {
            scan_select_rows(&mut self.pager, &table, &projection_indices, None, select.limit)?
        };

        let stats = QueryStats {
            latency_ms: started.elapsed().as_secs_f64() * 1000.0,
            rows_read,
            rows_returned: rows.len(),
            used_index: plan.used_index.is_some(),
        };

        Ok(ExecutionEnvelope {
            ast,
            plan,
            columns,
            rows,
            stats,
            message: None,
        })
    }

    fn run_mutation<F>(
        &mut self,
        ast: Statement,
        plan: Plan,
        operation: F,
    ) -> Result<ExecutionEnvelope>
    where
        F: FnOnce(&mut Self) -> Result<String>,
    {
        let started = Instant::now();
        let message = operation(self);
        match message {
            Ok(message) => {
                if !self.in_transaction {
                    if let Err(error) = self.pager.commit() {
                        let _ = self.pager.rollback();
                        let _ = self.reload_catalog();
                        return Err(error);
                    }
                    self.reload_catalog()?;
                }
                Ok(ExecutionEnvelope {
                    ast,
                    plan,
                    columns: Vec::new(),
                    rows: Vec::new(),
                    stats: QueryStats {
                        latency_ms: started.elapsed().as_secs_f64() * 1000.0,
                        rows_read: 0,
                        rows_returned: 0,
                        used_index: false,
                    },
                    message: Some(message),
                })
            }
            Err(error) => {
                if !self.in_transaction {
                    let _ = self.pager.rollback();
                    let _ = self.reload_catalog();
                }
                Err(error)
            }
        }
    }

    fn create_table(&mut self, create: &CreateTableStatement) -> Result<String> {
        if self.catalog.table(&create.table_name).is_some() {
            return Err(Error::Catalog(format!("table {} already exists", create.table_name)));
        }
        if create.columns.is_empty() {
            return Err(Error::Catalog("CREATE TABLE requires at least one column".into()));
        }
        let mut seen = std::collections::BTreeSet::new();
        for column in &create.columns {
            let lowered = column.name.to_ascii_lowercase();
            if !seen.insert(lowered) {
                return Err(Error::Catalog(format!(
                    "duplicate column {} in CREATE TABLE",
                    column.name
                )));
            }
        }
        let first_heap_page = allocate_heap_page(&mut self.pager)?;
        let table = TableMeta {
            id: self.catalog.next_table_id,
            name: create.table_name.clone(),
            columns: create.columns.clone(),
            first_heap_page,
            last_heap_page: first_heap_page,
        };
        self.catalog.next_table_id += 1;
        self.catalog.tables.push(table);
        self.persist_catalog()?;
        Ok(format!("table {} created", create.table_name))
    }

    fn insert(&mut self, insert: &InsertStatement) -> Result<String> {
        let (table_id, row_id) = {
            let table = self
                .catalog
                .table_mut(&insert.table_name)
                .ok_or_else(|| Error::Catalog(format!("table {} not found", insert.table_name)))?;
            let row_id = insert_row(&mut self.pager, table, &insert.values)?;
            (table.id, row_id)
        };
        self.rebuild_indexes_for_table(table_id)?;
        self.persist_catalog()?;
        Ok(format!(
            "1 row inserted at page {} slot {}",
            row_id.page_id, row_id.slot_id
        ))
    }

    fn delete(&mut self, delete: &DeleteStatement) -> Result<String> {
        let table = self
            .catalog
            .table(&delete.table_name)
            .ok_or_else(|| Error::Catalog(format!("table {} not found", delete.table_name)))?
            .clone();
        let candidate_row_ids = if let Some(filter) = &delete.filter {
            if let Some(index_meta) = self.catalog.index_for_column(table.id, &filter.column) {
                if index_meta.key_type == filter.value.value_type() {
                    search_index(&mut self.pager, index_meta.root_page_id, &filter.value)?.row_ids
                } else {
                    collect_scan_row_ids(&mut self.pager, &table, delete.filter.as_ref())?
                }
            } else {
                collect_scan_row_ids(&mut self.pager, &table, delete.filter.as_ref())?
            }
        } else {
            collect_scan_row_ids(&mut self.pager, &table, None)?
        };

        let mut deleted = 0usize;
        for row_id in candidate_row_ids {
            if let Some(row) = fetch_row(&mut self.pager, &table, row_id)? {
                if delete
                    .filter
                    .as_ref()
                    .map(|filter| value_matches_filter(&row.values, &table, filter))
                    .transpose()?
                    .unwrap_or(true)
                {
                    mark_deleted(&mut self.pager, row_id)?;
                    deleted += 1;
                }
            }
        }
        self.rebuild_indexes_for_table(table.id)?;
        self.persist_catalog()?;
        Ok(format!("{deleted} row(s) deleted"))
    }

    fn create_index(&mut self, create_index: &CreateIndexStatement) -> Result<String> {
        if self
            .catalog
            .indexes
            .iter()
            .any(|index| index.name.eq_ignore_ascii_case(&create_index.index_name))
        {
            return Err(Error::Catalog(format!(
                "index {} already exists",
                create_index.index_name
            )));
        }
        let table = self
            .catalog
            .table(&create_index.table_name)
            .ok_or_else(|| Error::Catalog(format!("table {} not found", create_index.table_name)))?
            .clone();
        let column_idx = column_index(&table, &create_index.column_name)?;
        let entries = scan_rows(&mut self.pager, &table)?
            .into_iter()
            .map(|row| (row.values[column_idx].clone(), row.row_id))
            .collect::<Vec<_>>();
        let root_page_id = build_index_pages(&mut self.pager, entries)?;
        self.catalog.indexes.push(IndexMeta {
            id: self.catalog.next_index_id,
            name: create_index.index_name.clone(),
            table_id: table.id,
            column_name: create_index.column_name.clone(),
            root_page_id,
            key_type: table.columns[column_idx].column_type,
        });
        self.catalog.next_index_id += 1;
        self.persist_catalog()?;
        Ok(format!("index {} created", create_index.index_name))
    }

    fn rebuild_indexes_for_table(&mut self, table_id: u32) -> Result<()> {
        let table = match self.catalog.table_by_id(table_id) {
            Some(table) => table.clone(),
            None => return Ok(()),
        };
        let rows = scan_rows(&mut self.pager, &table)?;
        let index_positions = self
            .catalog
            .indexes
            .iter()
            .enumerate()
            .filter_map(|(position, index)| (index.table_id == table_id).then_some(position))
            .collect::<Vec<_>>();
        for position in index_positions {
            let column_name = self.catalog.indexes[position].column_name.clone();
            let column_idx = column_index(&table, &column_name)?;
            let entries = rows
                .iter()
                .map(|row| (row.values[column_idx].clone(), row.row_id))
                .collect::<Vec<_>>();
            let root_page_id = build_index_pages(&mut self.pager, entries)?;
            self.catalog.indexes[position].root_page_id = root_page_id;
        }
        Ok(())
    }

    fn persist_catalog(&mut self) -> Result<()> {
        self.pager.replace_catalog(&self.catalog.serialize()?)
    }

    fn reload_catalog(&mut self) -> Result<()> {
        self.catalog = Catalog::deserialize(&self.pager.read_catalog()?)?;
        Ok(())
    }
}

pub fn run_benchmark(path: &Path) -> Result<String> {
    let mut database = Database::open(path)?;
    if database.catalog.table("bench_users").is_none() {
        database.execute_statement(
            parse_statement("CREATE TABLE bench_users (id INT, name TEXT);")?,
            OutputFormat::Table,
            false,
        )?;
        for index in 0..500 {
            let query = format!(
                "INSERT INTO bench_users VALUES ({}, 'user_{}');",
                index, index
            );
            database.execute_statement(parse_statement(&query)?, OutputFormat::Table, false)?;
        }
        database.execute_statement(
            parse_statement("CREATE INDEX idx_bench_users_id ON bench_users(id);")?,
            OutputFormat::Table,
            false,
        )?;
    }

    let scan = database.execute_statement(
        parse_statement("SELECT * FROM bench_users WHERE name = 'user_420';")?,
        OutputFormat::Json,
        false,
    )?;
    let index = database.execute_statement(
        parse_statement("SELECT * FROM bench_users WHERE id = 420;")?,
        OutputFormat::Json,
        false,
    )?;
    Ok(format!("scan={}\nindex={}", scan.rendered, index.rendered))
}

fn scan_select_rows(
    pager: &mut Pager,
    table: &TableMeta,
    projection_indices: &[usize],
    filter: Option<&crate::common::Filter>,
    limit: Option<usize>,
) -> Result<(usize, Vec<Vec<Value>>)> {
    let rows = scan_rows(pager, table)?;
    let mut materialized = Vec::new();
    let mut rows_read = 0usize;
    for row in rows {
        rows_read += 1;
        if filter
            .map(|filter| value_matches_filter(&row.values, table, filter))
            .transpose()?
            .unwrap_or(true)
        {
            materialized.push(project_values(&row.values, projection_indices));
            if limit.is_some_and(|value| materialized.len() >= value) {
                break;
            }
        }
    }
    Ok((rows_read, materialized))
}

fn collect_scan_row_ids(
    pager: &mut Pager,
    table: &TableMeta,
    filter: Option<&crate::common::Filter>,
) -> Result<Vec<crate::common::RowId>> {
    let rows = scan_rows(pager, table)?;
    let mut row_ids = Vec::new();
    for row in rows {
        if filter
            .map(|filter| value_matches_filter(&row.values, table, filter))
            .transpose()?
            .unwrap_or(true)
        {
            row_ids.push(row.row_id);
        }
    }
    Ok(row_ids)
}

fn projection_indices(table: &TableMeta, projection: &Projection) -> Result<Vec<usize>> {
    match projection {
        Projection::All => Ok((0..table.columns.len()).collect()),
        Projection::Columns(columns) => columns.iter().map(|column| column_index(table, column)).collect(),
    }
}

fn value_matches_filter(
    values: &[Value],
    table: &TableMeta,
    filter: &crate::common::Filter,
) -> Result<bool> {
    let index = column_index(table, &filter.column)?;
    Ok(values.get(index) == Some(&filter.value))
}

fn project_values(values: &[Value], projection_indices: &[usize]) -> Vec<Value> {
    projection_indices
        .iter()
        .map(|index| values[*index].clone())
        .collect()
}

fn render_table_output(envelope: &ExecutionEnvelope) -> String {
    let mut lines = Vec::new();
    lines.push(format!("plan: {}", envelope.plan.to_text()));
    if let Some(message) = &envelope.message {
        lines.push(format!("message: {message}"));
    }
    if !envelope.columns.is_empty() {
        lines.push(envelope.columns.join(" | "));
        for row in &envelope.rows {
            lines.push(
                row.iter()
                    .map(Value::display_value)
                    .collect::<Vec<_>>()
                    .join(" | "),
            );
        }
    }
    lines.push(format!(
        "stats: {:.3} ms, rows_read={}, rows_returned={}, used_index={}",
        envelope.stats.latency_ms,
        envelope.stats.rows_read,
        envelope.stats.rows_returned,
        envelope.stats.used_index
    ));
    lines.join("\n")
}

fn render_json_output(envelope: &ExecutionEnvelope) -> String {
    render_json_statement(envelope)
}

fn render_json_batch_output(envelopes: &[ExecutionEnvelope]) -> String {
    if envelopes.len() == 1 {
        return render_json_statement(&envelopes[0]);
    }
    let final_result = envelopes
        .last()
        .map(render_json_statement)
        .unwrap_or_else(|| "{\"error\":null}".into());
    format!(
        "{{\"statements\":[{}],\"final\":{},\"error\":null}}",
        envelopes
            .iter()
            .map(render_json_statement)
            .collect::<Vec<_>>()
            .join(","),
        final_result
    )
}

fn render_json_statement(envelope: &ExecutionEnvelope) -> String {
    format!(
        "{{\"ast\":{},\"plan\":{},\"columns\":[{}],\"rows\":[{}],\"stats\":{{\"latency_ms\":{:.3},\"rows_read\":{},\"rows_returned\":{},\"used_index\":{}}},\"error\":null{}}}",
        statement_to_json(&envelope.ast),
        envelope.plan.to_json(),
        envelope
            .columns
            .iter()
            .map(|column| format!("\"{}\"", escape_json(column)))
            .collect::<Vec<_>>()
            .join(","),
        envelope
            .rows
            .iter()
            .map(|row| {
                format!(
                    "[{}]",
                    row.iter().map(Value::to_json).collect::<Vec<_>>().join(",")
                )
            })
            .collect::<Vec<_>>()
            .join(","),
        envelope.stats.latency_ms,
        envelope.stats.rows_read,
        envelope.stats.rows_returned,
        if envelope.stats.used_index { "true" } else { "false" },
        envelope
            .message
            .as_ref()
            .map(|message| format!(",\"message\":\"{}\"", escape_json(message)))
            .unwrap_or_default()
    )
}

pub fn render_json_error(error: &Error) -> String {
    format!(
        "{{\"ast\":null,\"plan\":null,\"columns\":[],\"rows\":[],\"stats\":null,\"error\":{{\"kind\":\"{}\",\"message\":\"{}\"}}}}",
        error_kind(error),
        escape_json(&error.to_string())
    )
}

fn statement_to_json(statement: &Statement) -> String {
    match statement {
        Statement::CreateTable(create) => format!(
            "{{\"kind\":\"CreateTable\",\"table_name\":\"{}\",\"columns\":[{}]}}",
            escape_json(&create.table_name),
            create
                .columns
                .iter()
                .map(|column| format!(
                    "{{\"name\":\"{}\",\"type\":\"{}\"}}",
                    escape_json(&column.name),
                    column.column_type.as_str()
                ))
                .collect::<Vec<_>>()
                .join(",")
        ),
        Statement::Insert(insert) => format!(
            "{{\"kind\":\"Insert\",\"table_name\":\"{}\",\"values\":[{}]}}",
            escape_json(&insert.table_name),
            insert
                .values
                .iter()
                .map(Value::to_json)
                .collect::<Vec<_>>()
                .join(",")
        ),
        Statement::Select(select) => select_to_json("Select", select),
        Statement::Delete(delete) => format!(
            "{{\"kind\":\"Delete\",\"table_name\":\"{}\",\"filter\":{}}}",
            escape_json(&delete.table_name),
            delete
                .filter
                .as_ref()
                .map(filter_to_json)
                .unwrap_or_else(|| "null".into())
        ),
        Statement::CreateIndex(index) => format!(
            "{{\"kind\":\"CreateIndex\",\"index_name\":\"{}\",\"table_name\":\"{}\",\"column_name\":\"{}\"}}",
            escape_json(&index.index_name),
            escape_json(&index.table_name),
            escape_json(&index.column_name)
        ),
        Statement::Explain(select) => select_to_json("Explain", select),
        Statement::Begin => "{\"kind\":\"Begin\"}".into(),
        Statement::Commit => "{\"kind\":\"Commit\"}".into(),
        Statement::Rollback => "{\"kind\":\"Rollback\"}".into(),
        Statement::MetaCommand(_) => "{\"kind\":\"MetaCommand\",\"command\":\".exit\"}".into(),
    }
}

fn select_to_json(kind: &str, select: &SelectStatement) -> String {
    format!(
        "{{\"kind\":\"{}\",\"table_name\":\"{}\",\"projection\":{},\"filter\":{},\"limit\":{}}}",
        kind,
        escape_json(&select.table_name),
        match &select.projection {
            Projection::All => "\"*\"".into(),
            Projection::Columns(columns) => format!(
                "[{}]",
                columns
                    .iter()
                    .map(|column| format!("\"{}\"", escape_json(column)))
                    .collect::<Vec<_>>()
                    .join(",")
            ),
        },
        select
            .filter
            .as_ref()
            .map(filter_to_json)
            .unwrap_or_else(|| "null".into()),
        select
            .limit
            .map(|limit| limit.to_string())
            .unwrap_or_else(|| "null".into())
    )
}

fn filter_to_json(filter: &crate::common::Filter) -> String {
    format!(
        "{{\"column\":\"{}\",\"value\":{}}}",
        escape_json(&filter.column),
        filter.value.to_json()
    )
}

fn error_kind(error: &Error) -> &'static str {
    match error {
        Error::Message(_) => "Message",
        Error::Io(_) => "Io",
        Error::Parse(_) => "Parse",
        Error::Catalog(_) => "Catalog",
        Error::Storage(_) => "Storage",
        Error::Execution(_) => "Execution",
        Error::Transaction(_) => "Transaction",
    }
}
