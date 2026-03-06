use crate::common::{Error, Filter, FilterOp, Result, RowId, Value};
use crate::storage::page::{blank_page, PAGE_SIZE, PAGE_TYPE_INDEX_INTERNAL, PAGE_TYPE_INDEX_LEAF};
use crate::storage::pager::Pager;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SearchResult {
    pub row_ids: Vec<RowId>,
}

#[derive(Debug, Clone)]
struct ChildPointer {
    max_key: Value,
    page_id: u32,
}

pub fn build_index_pages(
    pager: &mut Pager,
    mut entries: Vec<(Value, RowId)>,
) -> Result<u32> {
    if entries.is_empty() {
        let page_id = pager.allocate_page(encode_leaf_page(&[])? )?;
        return Ok(page_id);
    }

    entries.sort_by(|left, right| left.0.cmp(&right.0).then(left.1.cmp(&right.1)));
    let grouped = group_entries(entries);
    let mut current_level = build_leaf_level(pager, &grouped)?;
    while current_level.len() > 1 {
        current_level = build_internal_level(pager, &current_level)?;
    }
    Ok(current_level[0].page_id)
}

pub fn search_index(pager: &mut Pager, root_page: u32, needle: &Value) -> Result<SearchResult> {
    let leaf_page = find_leaf_page(pager, root_page, needle)?;
    let page = pager.get_page(leaf_page)?;
    search_leaf(&page, needle)
}

pub fn search_index_range(
    pager: &mut Pager,
    root_page: u32,
    filter: &Filter,
) -> Result<SearchResult> {
    let mut current_page = match filter.op {
        FilterOp::Eq => find_leaf_page(pager, root_page, &filter.value)?,
        FilterOp::Gt | FilterOp::Gte => find_leaf_page(pager, root_page, &filter.value)?,
        FilterOp::Lt | FilterOp::Lte => leftmost_leaf_page(pager, root_page)?,
    };
    let mut row_ids = Vec::new();

    while current_page != 0 {
        let page = pager.get_page(current_page)?;
        let leaf = decode_leaf(&page)?;
        for (key, key_row_ids) in leaf.entries {
            if should_stop_scan(&filter.op, &key, &filter.value) {
                return Ok(SearchResult { row_ids });
            }
            if filter.op.matches(&key, &filter.value) {
                row_ids.extend(key_row_ids);
            }
        }
        current_page = leaf.next_page_id;
    }

    Ok(SearchResult { row_ids })
}

fn build_leaf_level(pager: &mut Pager, grouped: &[(Value, Vec<RowId>)]) -> Result<Vec<ChildPointer>> {
    let mut pages = Vec::new();
    let mut start = 0usize;
    while start < grouped.len() {
        if encoded_leaf_entry_size(&grouped[start].0, &grouped[start].1) + 7 > PAGE_SIZE {
            return Err(Error::Storage("index entry too large for a leaf page".into()));
        }
        let mut chunk_len = 0usize;
        let mut end = start;
        while end < grouped.len() {
            let next_len = chunk_len + encoded_leaf_entry_size(&grouped[end].0, &grouped[end].1);
            if next_len + 7 > PAGE_SIZE && end > start {
                break;
            }
            chunk_len = next_len;
            end += 1;
        }
        let page_id = pager.allocate_page(encode_leaf_page(&grouped[start..end])?)?;
        pages.push(ChildPointer {
            max_key: grouped[end - 1].0.clone(),
            page_id,
        });
        start = end;
    }

    for window in pages.windows(2) {
        let current = &window[0];
        let next = &window[1];
        let mut page = pager.get_page(current.page_id)?;
        page[1..5].copy_from_slice(&next.page_id.to_le_bytes());
        pager.write_page(current.page_id, page)?;
    }

    Ok(pages)
}

fn build_internal_level(pager: &mut Pager, children: &[ChildPointer]) -> Result<Vec<ChildPointer>> {
    let mut parents = Vec::new();
    let mut start = 0usize;
    while start < children.len() {
        if encoded_internal_entry_size(&children[start]) + 3 > PAGE_SIZE {
            return Err(Error::Storage("internal index entry too large for a page".into()));
        }
        let mut encoded = 0usize;
        let mut end = start;
        while end < children.len() {
            let next_len = encoded + encoded_internal_entry_size(&children[end]);
            if next_len + 3 > PAGE_SIZE && end > start {
                break;
            }
            encoded = next_len;
            end += 1;
        }
        let page_id = pager.allocate_page(encode_internal_page(&children[start..end])?)?;
        parents.push(ChildPointer {
            max_key: children[end - 1].max_key.clone(),
            page_id,
        });
        start = end;
    }
    Ok(parents)
}

fn group_entries(entries: Vec<(Value, RowId)>) -> Vec<(Value, Vec<RowId>)> {
    let mut grouped: Vec<(Value, Vec<RowId>)> = Vec::new();
    for (key, row_id) in entries {
        if let Some((existing_key, row_ids)) = grouped.last_mut() {
            if existing_key == &key {
                row_ids.push(row_id);
                continue;
            }
        }
        grouped.push((key, vec![row_id]));
    }
    grouped
}

fn encode_leaf_page(entries: &[(Value, Vec<RowId>)]) -> Result<Vec<u8>> {
    let mut page = blank_page(PAGE_TYPE_INDEX_LEAF);
    page[1..5].copy_from_slice(&0u32.to_le_bytes());
    page[5..7].copy_from_slice(&(entries.len() as u16).to_le_bytes());
    let mut offset = 7usize;
    for (key, row_ids) in entries {
        offset += encode_key_into(&mut page[offset..], key)?;
        page[offset..offset + 2].copy_from_slice(&(row_ids.len() as u16).to_le_bytes());
        offset += 2;
        for row_id in row_ids {
            page[offset..offset + 4].copy_from_slice(&row_id.page_id.to_le_bytes());
            page[offset + 4..offset + 6].copy_from_slice(&row_id.slot_id.to_le_bytes());
            offset += 6;
        }
    }
    Ok(page)
}

fn encode_internal_page(children: &[ChildPointer]) -> Result<Vec<u8>> {
    let mut page = blank_page(PAGE_TYPE_INDEX_INTERNAL);
    page[1..3].copy_from_slice(&(children.len() as u16).to_le_bytes());
    let mut offset = 3usize;
    for child in children {
        offset += encode_key_into(&mut page[offset..], &child.max_key)?;
        page[offset..offset + 4].copy_from_slice(&child.page_id.to_le_bytes());
        offset += 4;
    }
    Ok(page)
}

fn search_leaf(page: &[u8], needle: &Value) -> Result<SearchResult> {
    for (key, row_ids) in decode_leaf(page)?.entries {
        if &key == needle {
            return Ok(SearchResult { row_ids });
        }
    }
    Ok(SearchResult { row_ids: Vec::new() })
}

fn search_internal(page: &[u8], needle: &Value) -> Result<u32> {
    let entry_count = u16::from_le_bytes(page[1..3].try_into().unwrap()) as usize;
    let mut offset = 3usize;
    let mut fallback = None;
    for _ in 0..entry_count {
        let (max_key, consumed) = decode_key(&page[offset..])?;
        offset += consumed;
        let child_page = u32::from_le_bytes(page[offset..offset + 4].try_into().unwrap());
        offset += 4;
        fallback = Some(child_page);
        if needle <= &max_key {
            return Ok(child_page);
        }
    }
    fallback.ok_or_else(|| Error::Storage("empty internal index page".into()))
}

fn find_leaf_page(pager: &mut Pager, root_page: u32, needle: &Value) -> Result<u32> {
    let mut current_page = root_page;
    loop {
        let page = pager.get_page(current_page)?;
        match page[0] {
            PAGE_TYPE_INDEX_LEAF => return Ok(current_page),
            PAGE_TYPE_INDEX_INTERNAL => {
                current_page = search_internal(&page, needle)?;
            }
            other => {
                return Err(Error::Storage(format!(
                    "unexpected index page type {other}"
                )))
            }
        }
    }
}

fn leftmost_leaf_page(pager: &mut Pager, root_page: u32) -> Result<u32> {
    let mut current_page = root_page;
    loop {
        let page = pager.get_page(current_page)?;
        match page[0] {
            PAGE_TYPE_INDEX_LEAF => return Ok(current_page),
            PAGE_TYPE_INDEX_INTERNAL => {
                current_page = first_child_page(&page)?;
            }
            other => {
                return Err(Error::Storage(format!(
                    "unexpected index page type {other}"
                )))
            }
        }
    }
}

fn first_child_page(page: &[u8]) -> Result<u32> {
    let entry_count = u16::from_le_bytes(page[1..3].try_into().unwrap()) as usize;
    if entry_count == 0 {
        return Err(Error::Storage("empty internal index page".into()));
    }
    let (_, consumed) = decode_key(&page[3..])?;
    let offset = 3 + consumed;
    Ok(u32::from_le_bytes(page[offset..offset + 4].try_into().unwrap()))
}

fn should_stop_scan(op: &FilterOp, key: &Value, bound: &Value) -> bool {
    match op {
        FilterOp::Eq => key > bound,
        FilterOp::Gt | FilterOp::Gte => false,
        FilterOp::Lt => key >= bound,
        FilterOp::Lte => key > bound,
    }
}

#[derive(Debug, Clone)]
struct LeafPage {
    next_page_id: u32,
    entries: Vec<(Value, Vec<RowId>)>,
}

fn decode_leaf(page: &[u8]) -> Result<LeafPage> {
    let entry_count = u16::from_le_bytes(page[5..7].try_into().unwrap()) as usize;
    let next_page_id = u32::from_le_bytes(page[1..5].try_into().unwrap());
    let mut offset = 7usize;
    let mut entries = Vec::with_capacity(entry_count);
    for _ in 0..entry_count {
        let (key, consumed) = decode_key(&page[offset..])?;
        offset += consumed;
        let row_count = u16::from_le_bytes(page[offset..offset + 2].try_into().unwrap()) as usize;
        offset += 2;
        let mut row_ids = Vec::with_capacity(row_count);
        for _ in 0..row_count {
            let page_id = u32::from_le_bytes(page[offset..offset + 4].try_into().unwrap());
            let slot_id = u16::from_le_bytes(page[offset + 4..offset + 6].try_into().unwrap());
            offset += 6;
            row_ids.push(RowId { page_id, slot_id });
        }
        entries.push((key, row_ids));
    }
    Ok(LeafPage {
        next_page_id,
        entries,
    })
}

fn encode_key_into(buffer: &mut [u8], key: &Value) -> Result<usize> {
    match key {
        Value::Int(value) => {
            if buffer.len() < 9 {
                return Err(Error::Storage("insufficient space for INT index key".into()));
            }
            buffer[0] = 1;
            buffer[1..9].copy_from_slice(&value.to_le_bytes());
            Ok(9)
        }
        Value::Text(value) => {
            let bytes = value.as_bytes();
            if bytes.len() > u16::MAX as usize {
                return Err(Error::Storage("index key text too large".into()));
            }
             if buffer.len() < 3 + bytes.len() {
                return Err(Error::Storage("insufficient space for TEXT index key".into()));
            }
            buffer[0] = 2;
            buffer[1..3].copy_from_slice(&(bytes.len() as u16).to_le_bytes());
            buffer[3..3 + bytes.len()].copy_from_slice(bytes);
            Ok(3 + bytes.len())
        }
    }
}

fn decode_key(buffer: &[u8]) -> Result<(Value, usize)> {
    match buffer.first().copied().unwrap_or_default() {
        1 => {
            let bytes = buffer
                .get(1..9)
                .ok_or_else(|| Error::Storage("truncated INT index key".into()))?;
            Ok((Value::Int(i64::from_le_bytes(bytes.try_into().unwrap())), 9))
        }
        2 => {
            let len_slice = buffer
                .get(1..3)
                .ok_or_else(|| Error::Storage("truncated TEXT index key".into()))?;
            let length = u16::from_le_bytes(len_slice.try_into().unwrap()) as usize;
            let payload = buffer
                .get(3..3 + length)
                .ok_or_else(|| Error::Storage("truncated TEXT key payload".into()))?;
            let text = String::from_utf8(payload.to_vec())
                .map_err(|_| Error::Storage("invalid utf-8 in index key".into()))?;
            Ok((Value::Text(text), 3 + length))
        }
        _ => Err(Error::Storage("unknown index key tag".into())),
    }
}

fn encoded_leaf_entry_size(key: &Value, row_ids: &[RowId]) -> usize {
    encoded_key_size(key) + 2 + row_ids.len() * 6
}

fn encoded_internal_entry_size(child: &ChildPointer) -> usize {
    encoded_key_size(&child.max_key) + 4
}

fn encoded_key_size(key: &Value) -> usize {
    match key {
        Value::Int(_) => 9,
        Value::Text(value) => 3 + value.len(),
    }
}
