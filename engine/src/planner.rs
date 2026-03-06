use crate::ast::{DeleteStatement, Projection, SelectStatement, Statement, UpdateStatement};
use crate::catalog::Catalog;
use crate::common::{escape_json, Error, Filter, FilterOp, Result};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PlanKind {
    SeqScan,
    IndexLookup,
    IndexRangeScan,
    Explain,
    Mutation,
    Transaction,
    Ddl,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Plan {
    pub kind: PlanKind,
    pub table: Option<String>,
    pub projection: Vec<String>,
    pub filter: Option<Filter>,
    pub limit: Option<usize>,
    pub used_index: Option<String>,
    pub description: String,
}

impl Plan {
    pub fn to_text(&self) -> String {
        let mut parts = vec![format!("kind={:?}", self.kind)];
        if let Some(table) = &self.table {
            parts.push(format!("table={table}"));
        }
        if let Some(index) = &self.used_index {
            parts.push(format!("index={index}"));
        }
        if let Some(filter) = &self.filter {
            parts.push(format!(
                "filter={}{}{}",
                filter.column,
                filter.op.symbol(),
                filter.value.display_value()
            ));
        }
        if let Some(limit) = self.limit {
            parts.push(format!("limit={limit}"));
        }
        parts.push(self.description.clone());
        parts.join(" | ")
    }

    pub fn to_json(&self) -> String {
        format!(
            "{{\"kind\":\"{:?}\",\"table\":{},\"projection\":[{}],\"filter\":{},\"limit\":{},\"used_index\":{},\"description\":\"{}\"}}",
            self.kind,
            self.table
                .as_ref()
                .map(|value| format!("\"{}\"", escape_json(value)))
                .unwrap_or_else(|| "null".into()),
            self.projection
                .iter()
                .map(|column| format!("\"{}\"", escape_json(column)))
                .collect::<Vec<_>>()
                .join(","),
            match &self.filter {
                Some(filter) => format!(
                    "{{\"column\":\"{}\",\"op\":\"{}\",\"value\":{}}}",
                    escape_json(&filter.column),
                    filter.op.symbol(),
                    filter.value.to_json()
                ),
                None => "null".into(),
            },
            self.limit
                .map(|limit| limit.to_string())
                .unwrap_or_else(|| "null".into()),
            self.used_index
                .as_ref()
                .map(|value| format!("\"{}\"", escape_json(value)))
                .unwrap_or_else(|| "null".into()),
            escape_json(&self.description)
        )
    }
}

pub fn build_plan(statement: &Statement, catalog: &Catalog) -> Result<Plan> {
    match statement {
        Statement::Select(select) => build_select_plan(select, catalog),
        Statement::Delete(delete) => build_delete_plan(delete, catalog),
        Statement::Update(update) => build_update_plan(update, catalog),
        Statement::Explain(select) => {
            let mut plan = build_select_plan(select, catalog)?;
            plan.kind = PlanKind::Explain;
            plan.description = format!("EXPLAIN {}", plan.description);
            Ok(plan)
        }
        Statement::CreateTable(_) | Statement::CreateIndex(_) | Statement::Insert(_) => Ok(Plan {
            kind: PlanKind::Ddl,
            table: None,
            projection: Vec::new(),
            filter: None,
            limit: None,
            used_index: None,
            description: "mutation statement".into(),
        }),
        Statement::Begin | Statement::Commit | Statement::Rollback => Ok(Plan {
            kind: PlanKind::Transaction,
            table: None,
            projection: Vec::new(),
            filter: None,
            limit: None,
            used_index: None,
            description: "transaction control".into(),
        }),
        Statement::MetaCommand(_) => Ok(Plan {
            kind: PlanKind::Mutation,
            table: None,
            projection: Vec::new(),
            filter: None,
            limit: None,
            used_index: None,
            description: "meta command".into(),
        }),
    }
}

fn build_update_plan(statement: &UpdateStatement, catalog: &Catalog) -> Result<Plan> {
    let table = catalog
        .table(&statement.table_name)
        .ok_or_else(|| Error::Catalog(format!("table {} not found", statement.table_name)))?;
    let used_index = statement.filter.as_ref().and_then(|filter| {
        catalog
            .index_for_column(table.id, &filter.column)
            .filter(|index| index.key_type == filter.value.value_type())
            .map(|index| index.name.clone())
    });
    let (kind, description) = if let Some(index_name) = &used_index {
        match statement.filter.as_ref().map(|filter| filter.op) {
            Some(FilterOp::Eq) => (
                PlanKind::IndexLookup,
                format!("update via index {index_name}"),
            ),
            Some(_) => (
                PlanKind::IndexRangeScan,
                format!("update via index range scan {index_name}"),
            ),
            None => (PlanKind::SeqScan, "update via sequential scan".into()),
        }
    } else {
        (PlanKind::SeqScan, "update via sequential scan".into())
    };
    Ok(Plan {
        kind,
        table: Some(statement.table_name.clone()),
        projection: vec![statement.column_name.clone()],
        filter: statement.filter.clone(),
        limit: None,
        used_index,
        description,
    })
}

fn build_select_plan(statement: &SelectStatement, catalog: &Catalog) -> Result<Plan> {
    let table = catalog
        .table(&statement.table_name)
        .ok_or_else(|| Error::Catalog(format!("table {} not found", statement.table_name)))?;
    let projection = match &statement.projection {
        Projection::All => table.columns.iter().map(|column| column.name.clone()).collect(),
        Projection::Columns(columns) => columns.clone(),
    };
    let used_index = statement.filter.as_ref().and_then(|filter| {
        catalog
            .index_for_column(table.id, &filter.column)
            .filter(|index| index.key_type == filter.value.value_type())
            .map(|index| index.name.clone())
    });
    let (kind, description) = if let Some(index_name) = &used_index {
        match statement.filter.as_ref().map(|filter| filter.op) {
            Some(FilterOp::Eq) => (
                PlanKind::IndexLookup,
                format!("index lookup via {index_name}"),
            ),
            Some(_) => (
                PlanKind::IndexRangeScan,
                format!("index range scan via {index_name}"),
            ),
            None => (PlanKind::SeqScan, "sequential heap scan".into()),
        }
    } else {
        (PlanKind::SeqScan, "sequential heap scan".into())
    };
    Ok(Plan {
        kind,
        table: Some(statement.table_name.clone()),
        projection,
        filter: statement.filter.clone(),
        limit: statement.limit,
        used_index,
        description,
    })
}

fn build_delete_plan(statement: &DeleteStatement, catalog: &Catalog) -> Result<Plan> {
    let table = catalog
        .table(&statement.table_name)
        .ok_or_else(|| Error::Catalog(format!("table {} not found", statement.table_name)))?;
    let used_index = statement.filter.as_ref().and_then(|filter| {
        catalog
            .index_for_column(table.id, &filter.column)
            .filter(|index| index.key_type == filter.value.value_type())
            .map(|index| index.name.clone())
    });
    let (kind, description) = if let Some(index_name) = &used_index {
        match statement.filter.as_ref().map(|filter| filter.op) {
            Some(FilterOp::Eq) => (
                PlanKind::IndexLookup,
                format!("delete via index {index_name}"),
            ),
            Some(_) => (
                PlanKind::IndexRangeScan,
                format!("delete via index range scan {index_name}"),
            ),
            None => (PlanKind::SeqScan, "delete via sequential scan".into()),
        }
    } else {
        (PlanKind::SeqScan, "delete via sequential scan".into())
    };
    Ok(Plan {
        kind,
        table: Some(statement.table_name.clone()),
        projection: Vec::new(),
        filter: statement.filter.clone(),
        limit: None,
        used_index,
        description,
    })
}
