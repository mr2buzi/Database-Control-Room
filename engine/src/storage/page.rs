use crate::common::{Error, Result};

pub const PAGE_SIZE: usize = 4096;
pub const HEADER_MAGIC: &[u8; 8] = b"SLATEDB\0";
pub const PAGE_TYPE_CATALOG: u8 = 1;
pub const PAGE_TYPE_HEAP: u8 = 2;
pub const PAGE_TYPE_INDEX_LEAF: u8 = 3;
pub const PAGE_TYPE_INDEX_INTERNAL: u8 = 4;

const HEADER_VERSION: u32 = 1;
const HEAP_HEADER_SIZE: usize = 12;
const SLOT_SIZE: usize = 4;

#[derive(Debug, Clone)]
pub struct DbHeader {
    pub version: u32,
    pub page_size: u32,
    pub next_page_id: u32,
    pub catalog_start_page: u32,
    pub catalog_page_count: u32,
}

impl DbHeader {
    pub fn new() -> Self {
        Self {
            version: HEADER_VERSION,
            page_size: PAGE_SIZE as u32,
            next_page_id: 2,
            catalog_start_page: 1,
            catalog_page_count: 1,
        }
    }

    pub fn encode(&self) -> Vec<u8> {
        let mut page = vec![0u8; PAGE_SIZE];
        page[0..8].copy_from_slice(HEADER_MAGIC);
        page[8..12].copy_from_slice(&self.version.to_le_bytes());
        page[12..16].copy_from_slice(&self.page_size.to_le_bytes());
        page[16..20].copy_from_slice(&self.next_page_id.to_le_bytes());
        page[20..24].copy_from_slice(&self.catalog_start_page.to_le_bytes());
        page[24..28].copy_from_slice(&self.catalog_page_count.to_le_bytes());
        page
    }

    pub fn decode(bytes: &[u8]) -> Result<Self> {
        if bytes.len() != PAGE_SIZE {
            return Err(Error::Storage("invalid header page size".into()));
        }
        if &bytes[0..8] != HEADER_MAGIC {
            return Err(Error::Storage("invalid database header magic".into()));
        }
        Ok(Self {
            version: u32::from_le_bytes(bytes[8..12].try_into().unwrap()),
            page_size: u32::from_le_bytes(bytes[12..16].try_into().unwrap()),
            next_page_id: u32::from_le_bytes(bytes[16..20].try_into().unwrap()),
            catalog_start_page: u32::from_le_bytes(bytes[20..24].try_into().unwrap()),
            catalog_page_count: u32::from_le_bytes(bytes[24..28].try_into().unwrap()),
        })
    }
}

pub fn blank_page(page_type: u8) -> Vec<u8> {
    let mut page = vec![0u8; PAGE_SIZE];
    page[0] = page_type;
    page
}

pub fn catalog_page(payload: &[u8]) -> Result<Vec<u8>> {
    if payload.len() > PAGE_SIZE - 3 {
        return Err(Error::Storage("catalog page payload too large".into()));
    }
    let mut page = blank_page(PAGE_TYPE_CATALOG);
    page[1..3].copy_from_slice(&(payload.len() as u16).to_le_bytes());
    page[3..3 + payload.len()].copy_from_slice(payload);
    Ok(page)
}

pub fn catalog_payload(page: &[u8]) -> Result<Vec<u8>> {
    ensure_page_type(page, PAGE_TYPE_CATALOG)?;
    let length = u16::from_le_bytes(page[1..3].try_into().unwrap()) as usize;
    Ok(page[3..3 + length].to_vec())
}

pub fn init_heap_page(page: &mut [u8]) {
    page.fill(0);
    page[0] = PAGE_TYPE_HEAP;
    set_heap_next_page(page, 0);
    set_slot_count(page, 0);
    set_free_start(page, HEAP_HEADER_SIZE as u16);
    set_free_end(page, PAGE_SIZE as u16);
}

pub fn ensure_page_type(page: &[u8], expected: u8) -> Result<()> {
    let actual = page.first().copied().unwrap_or_default();
    if actual != expected {
        return Err(Error::Storage(format!(
            "page type mismatch, expected {expected}, found {actual}"
        )));
    }
    Ok(())
}

pub fn heap_next_page(page: &[u8]) -> u32 {
    u32::from_le_bytes(page[1..5].try_into().unwrap())
}

pub fn set_heap_next_page(page: &mut [u8], next_page: u32) {
    page[1..5].copy_from_slice(&next_page.to_le_bytes());
}

pub fn slot_count(page: &[u8]) -> u16 {
    u16::from_le_bytes(page[5..7].try_into().unwrap())
}

fn set_slot_count(page: &mut [u8], value: u16) {
    page[5..7].copy_from_slice(&value.to_le_bytes());
}

fn free_start(page: &[u8]) -> u16 {
    u16::from_le_bytes(page[7..9].try_into().unwrap())
}

fn set_free_start(page: &mut [u8], value: u16) {
    page[7..9].copy_from_slice(&value.to_le_bytes());
}

fn free_end(page: &[u8]) -> u16 {
    u16::from_le_bytes(page[9..11].try_into().unwrap())
}

fn set_free_end(page: &mut [u8], value: u16) {
    page[9..11].copy_from_slice(&value.to_le_bytes());
}

fn slot_offset(slot_id: u16) -> usize {
    HEAP_HEADER_SIZE + (slot_id as usize * SLOT_SIZE)
}

pub fn heap_insert_record(page: &mut [u8], record: &[u8]) -> Option<u16> {
    let slot_count = slot_count(page);
    let free_start = free_start(page) as usize;
    let free_end = free_end(page) as usize;
    let needed = SLOT_SIZE + record.len();
    if free_end < free_start + needed {
        return None;
    }

    let new_free_end = free_end - record.len();
    page[new_free_end..free_end].copy_from_slice(record);

    let slot_id = slot_count;
    let slot_offset = slot_offset(slot_id);
    page[slot_offset..slot_offset + 2].copy_from_slice(&(new_free_end as u16).to_le_bytes());
    page[slot_offset + 2..slot_offset + 4].copy_from_slice(&(record.len() as u16).to_le_bytes());

    set_slot_count(page, slot_count + 1);
    set_free_start(page, (free_start + SLOT_SIZE) as u16);
    set_free_end(page, new_free_end as u16);
    Some(slot_id)
}

pub fn heap_read_record(page: &[u8], slot_id: u16) -> Option<Vec<u8>> {
    if slot_id >= slot_count(page) {
        return None;
    }
    let slot_offset = slot_offset(slot_id);
    let offset = u16::from_le_bytes(page[slot_offset..slot_offset + 2].try_into().unwrap()) as usize;
    let length =
        u16::from_le_bytes(page[slot_offset + 2..slot_offset + 4].try_into().unwrap()) as usize;
    if length == 0 {
        return None;
    }
    Some(page[offset..offset + length].to_vec())
}

pub fn heap_mark_deleted(page: &mut [u8], slot_id: u16) -> Result<()> {
    if slot_id >= slot_count(page) {
        return Err(Error::Storage("slot out of range".into()));
    }
    let slot_offset = slot_offset(slot_id);
    let offset = u16::from_le_bytes(page[slot_offset..slot_offset + 2].try_into().unwrap()) as usize;
    let length =
        u16::from_le_bytes(page[slot_offset + 2..slot_offset + 4].try_into().unwrap()) as usize;
    if length == 0 {
        return Err(Error::Storage("slot is empty".into()));
    }
    page[offset] = 1;
    Ok(())
}
