use std::collections::BTreeMap;
use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

use crate::common::{Error, Result};
use crate::storage::page::{blank_page, catalog_page, catalog_payload, DbHeader, PAGE_SIZE};
use crate::storage::recovery::recover;
use crate::storage::wal::Wal;

pub struct Pager {
    path: PathBuf,
    file: File,
    wal: Wal,
    cache: BTreeMap<u32, Vec<u8>>,
    dirty: BTreeMap<u32, Vec<u8>>,
    header: DbHeader,
    next_txid: u64,
}

impl Pager {
    pub fn open(path: &Path) -> Result<Self> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let wal = Wal::open(&path.with_extension("wal"))?;
        recover(path, &wal)?;

        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(path)?;
        let is_new = file.metadata()?.len() == 0;
        if is_new {
            let header = DbHeader::new();
            file.seek(SeekFrom::Start(0))?;
            file.write_all(&header.encode())?;
            file.write_all(&catalog_page(&[])? )?;
            file.sync_all()?;
        }

        let mut header_page = vec![0u8; PAGE_SIZE];
        file.seek(SeekFrom::Start(0))?;
        file.read_exact(&mut header_page)?;
        let header = DbHeader::decode(&header_page)?;

        let mut cache = BTreeMap::new();
        cache.insert(0, header_page);

        Ok(Self {
            path: path.to_path_buf(),
            file,
            wal,
            cache,
            dirty: BTreeMap::new(),
            header,
            next_txid: 1,
        })
    }

    pub fn header(&self) -> &DbHeader {
        &self.header
    }

    pub fn wal_path(&self) -> &Path {
        self.wal.path()
    }

    pub fn get_page(&mut self, page_id: u32) -> Result<Vec<u8>> {
        if let Some(page) = self.dirty.get(&page_id) {
            return Ok(page.clone());
        }
        if let Some(page) = self.cache.get(&page_id) {
            return Ok(page.clone());
        }
        let page = self.read_page_from_disk(page_id)?;
        self.cache.insert(page_id, page.clone());
        Ok(page)
    }

    pub fn write_page(&mut self, page_id: u32, bytes: Vec<u8>) -> Result<()> {
        if bytes.len() != PAGE_SIZE {
            return Err(Error::Storage("page write must be exactly 4096 bytes".into()));
        }
        if page_id == 0 {
            self.header = DbHeader::decode(&bytes)?;
        }
        self.dirty.insert(page_id, bytes);
        Ok(())
    }

    pub fn allocate_page(&mut self, initial: Vec<u8>) -> Result<u32> {
        if initial.len() != PAGE_SIZE {
            return Err(Error::Storage("allocated page must be exactly 4096 bytes".into()));
        }
        let page_id = self.header.next_page_id;
        self.header.next_page_id += 1;
        self.write_page(0, self.header.encode())?;
        self.write_page(page_id, initial)?;
        Ok(page_id)
    }

    pub fn replace_catalog(&mut self, payload: &[u8]) -> Result<()> {
        let chunk_size = PAGE_SIZE - 3;
        let required_pages = payload.len().div_ceil(chunk_size).max(1) as u32;
        let start_page = if required_pages <= self.header.catalog_page_count {
            self.header.catalog_start_page
        } else {
            let start = self.header.next_page_id;
            self.header.next_page_id += required_pages;
            start
        };

        for page_index in 0..required_pages {
            let start = page_index as usize * chunk_size;
            let end = ((page_index + 1) as usize * chunk_size).min(payload.len());
            let chunk = if start < end { &payload[start..end] } else { &[] };
            self.write_page(start_page + page_index, catalog_page(chunk)?)?;
        }

        self.header.catalog_start_page = start_page;
        self.header.catalog_page_count = required_pages;
        self.write_page(0, self.header.encode())?;
        Ok(())
    }

    pub fn read_catalog(&mut self) -> Result<Vec<u8>> {
        let mut payload = Vec::new();
        for page_offset in 0..self.header.catalog_page_count {
            let page = self.get_page(self.header.catalog_start_page + page_offset)?;
            payload.extend_from_slice(&catalog_payload(&page)?);
        }
        Ok(payload)
    }

    pub fn commit(&mut self) -> Result<()> {
        if self.dirty.is_empty() {
            return Ok(());
        }
        let txid = self.next_txid;
        self.next_txid += 1;
        let pages: Vec<(u32, Vec<u8>)> = self
            .dirty
            .iter()
            .map(|(page_id, bytes)| (*page_id, bytes.clone()))
            .collect();
        self.wal.append_transaction(txid, &pages)?;

        for (page_id, bytes) in &pages {
            self.file
                .seek(SeekFrom::Start(*page_id as u64 * PAGE_SIZE as u64))?;
            self.file.write_all(bytes)?;
            self.cache.insert(*page_id, bytes.clone());
        }
        self.file.sync_all()?;
        self.dirty.clear();
        Ok(())
    }

    pub fn rollback(&mut self) -> Result<()> {
        self.dirty.clear();
        let header_page = self.read_page_from_disk(0)?;
        self.header = DbHeader::decode(&header_page)?;
        self.cache.insert(0, header_page);
        Ok(())
    }

    pub fn file_path(&self) -> &Path {
        &self.path
    }

    fn read_page_from_disk(&mut self, page_id: u32) -> Result<Vec<u8>> {
        let offset = page_id as u64 * PAGE_SIZE as u64;
        let file_len = self.file.metadata()?.len();
        if offset >= file_len {
            return Ok(blank_page(0));
        }
        self.file.seek(SeekFrom::Start(offset))?;
        let mut page = vec![0u8; PAGE_SIZE];
        self.file.read_exact(&mut page)?;
        Ok(page)
    }
}
