use crate::catalog::TableMeta;
use crate::common::{Error, Result, RowId, Value};
use crate::storage::page::{
    blank_page, heap_insert_record, heap_mark_deleted, heap_next_page, heap_read_record,
    init_heap_page, PAGE_SIZE, PAGE_TYPE_HEAP,
};
use crate::storage::pager::Pager;
use crate::storage::row::{decode_row, encode_row};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TableRow {
    pub row_id: RowId,
    pub values: Vec<Value>,
}

pub fn allocate_heap_page(pager: &mut Pager) -> Result<u32> {
    let mut page = blank_page(PAGE_TYPE_HEAP);
    init_heap_page(&mut page);
    pager.allocate_page(page)
}

pub fn insert_row(pager: &mut Pager, table: &mut TableMeta, values: &[Value]) -> Result<RowId> {
    let encoded = encode_row(values, &table.columns)?;
    if encoded.len() + 16 > PAGE_SIZE {
        return Err(Error::Storage("row is too large for a single page".into()));
    }
    let mut page_id = table.last_heap_page;

    loop {
        let mut page = pager.get_page(page_id)?;
        if let Some(slot_id) = heap_insert_record(&mut page, &encoded) {
            pager.write_page(page_id, page)?;
            return Ok(RowId { page_id, slot_id });
        }

        let next_page = heap_next_page(&page);
        if next_page != 0 {
            page_id = next_page;
            continue;
        }

        let new_page_id = allocate_heap_page(pager)?;
        crate::storage::page::set_heap_next_page(&mut page, new_page_id);
        pager.write_page(page_id, page)?;
        table.last_heap_page = new_page_id;
        page_id = new_page_id;
    }
}

pub fn scan_rows(pager: &mut Pager, table: &TableMeta) -> Result<Vec<TableRow>> {
    let mut rows = Vec::new();
    let mut page_id = table.first_heap_page;
    while page_id != 0 {
        let page = pager.get_page(page_id)?;
        let slot_count = crate::storage::page::slot_count(&page);
        for slot_id in 0..slot_count {
            if let Some(bytes) = heap_read_record(&page, slot_id) {
                let decoded = decode_row(&bytes, &table.columns)?;
                if !decoded.deleted {
                    rows.push(TableRow {
                        row_id: RowId { page_id, slot_id },
                        values: decoded.values,
                    });
                }
            }
        }
        page_id = heap_next_page(&page);
    }
    Ok(rows)
}

pub fn fetch_row(pager: &mut Pager, table: &TableMeta, row_id: RowId) -> Result<Option<TableRow>> {
    let page = pager.get_page(row_id.page_id)?;
    let bytes = match heap_read_record(&page, row_id.slot_id) {
        Some(bytes) => bytes,
        None => return Ok(None),
    };
    let decoded = decode_row(&bytes, &table.columns)?;
    if decoded.deleted {
        return Ok(None);
    }
    Ok(Some(TableRow {
        row_id,
        values: decoded.values,
    }))
}

pub fn mark_deleted(pager: &mut Pager, row_id: RowId) -> Result<()> {
    let mut page = pager.get_page(row_id.page_id)?;
    heap_mark_deleted(&mut page, row_id.slot_id)?;
    pager.write_page(row_id.page_id, page)?;
    Ok(())
}

pub fn column_index(table: &TableMeta, column_name: &str) -> Result<usize> {
    table
        .columns
        .iter()
        .position(|column| column.name.eq_ignore_ascii_case(column_name))
        .ok_or_else(|| Error::Execution(format!("unknown column {column_name}")))
}
