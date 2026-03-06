use std::fs::OpenOptions;
use std::io::{Seek, SeekFrom, Write};
use std::path::Path;

use crate::common::Result;
use crate::storage::page::PAGE_SIZE;
use crate::storage::wal::Wal;

pub fn recover(data_path: &Path, wal: &Wal) -> Result<()> {
    let committed = wal.committed_page_sets()?;
    if committed.is_empty() {
        return Ok(());
    }

    let mut file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .open(data_path)?;
    for transaction in committed {
        for (page_id, bytes) in transaction {
            file.seek(SeekFrom::Start(page_id as u64 * PAGE_SIZE as u64))?;
            file.write_all(&bytes)?;
        }
    }
    file.sync_all()?;
    wal.truncate()?;
    Ok(())
}
