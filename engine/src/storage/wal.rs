use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

use crate::common::{Error, Result};
use crate::storage::page::PAGE_SIZE;

const WAL_MAGIC: &[u8; 8] = b"SLWAL1\0\0";
const RECORD_BEGIN: u8 = 1;
const RECORD_PAGE_IMAGE: u8 = 2;
const RECORD_COMMIT: u8 = 3;

#[derive(Debug, Clone)]
pub struct Wal {
    path: PathBuf,
}

impl Wal {
    pub fn open(path: &Path) -> Result<Self> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        if !path.exists() {
            let mut file = File::create(path)?;
            file.write_all(WAL_MAGIC)?;
            file.sync_all()?;
        }
        Ok(Self {
            path: path.to_path_buf(),
        })
    }

    pub fn append_transaction(&self, txid: u64, pages: &[(u32, Vec<u8>)]) -> Result<()> {
        let mut file = OpenOptions::new().append(true).open(&self.path)?;
        file.write_all(&[RECORD_BEGIN])?;
        file.write_all(&txid.to_le_bytes())?;
        for (page_id, bytes) in pages {
            if bytes.len() != PAGE_SIZE {
                return Err(Error::Storage("WAL page image size mismatch".into()));
            }
            file.write_all(&[RECORD_PAGE_IMAGE])?;
            file.write_all(&txid.to_le_bytes())?;
            file.write_all(&page_id.to_le_bytes())?;
            file.write_all(&(bytes.len() as u32).to_le_bytes())?;
            file.write_all(bytes)?;
        }
        file.write_all(&[RECORD_COMMIT])?;
        file.write_all(&txid.to_le_bytes())?;
        file.sync_all()?;
        Ok(())
    }

    pub fn committed_page_sets(&self) -> Result<Vec<Vec<(u32, Vec<u8>)>>> {
        let mut file = OpenOptions::new().read(true).open(&self.path)?;
        let mut magic = [0u8; 8];
        file.read_exact(&mut magic)?;
        if &magic != WAL_MAGIC {
            return Err(Error::Storage("invalid WAL header".into()));
        }
        let mut committed = Vec::new();
        let mut pending: std::collections::BTreeMap<u64, Vec<(u32, Vec<u8>)>> =
            std::collections::BTreeMap::new();
        loop {
            let mut record_type = [0u8; 1];
            match file.read_exact(&mut record_type) {
                Ok(()) => {}
                Err(error) if error.kind() == std::io::ErrorKind::UnexpectedEof => break,
                Err(error) => return Err(error.into()),
            }
            match record_type[0] {
                RECORD_BEGIN => {
                    let txid = read_u64(&mut file)?;
                    pending.entry(txid).or_default();
                }
                RECORD_PAGE_IMAGE => {
                    let txid = read_u64(&mut file)?;
                    let page_id = read_u32(&mut file)?;
                    let length = read_u32(&mut file)? as usize;
                    if length != PAGE_SIZE {
                        return Err(Error::Storage("invalid WAL page image length".into()));
                    }
                    let mut page = vec![0u8; length];
                    file.read_exact(&mut page)?;
                    pending.entry(txid).or_default().push((page_id, page));
                }
                RECORD_COMMIT => {
                    let txid = read_u64(&mut file)?;
                    if let Some(pages) = pending.remove(&txid) {
                        committed.push(pages);
                    }
                }
                _ => return Err(Error::Storage("unknown WAL record".into())),
            }
        }
        Ok(committed)
    }

    pub fn truncate(&self) -> Result<()> {
        let mut file = OpenOptions::new().write(true).truncate(true).open(&self.path)?;
        file.seek(SeekFrom::Start(0))?;
        file.write_all(WAL_MAGIC)?;
        file.sync_all()?;
        Ok(())
    }

    pub fn path(&self) -> &Path {
        &self.path
    }
}

fn read_u32(file: &mut File) -> Result<u32> {
    let mut bytes = [0u8; 4];
    file.read_exact(&mut bytes)?;
    Ok(u32::from_le_bytes(bytes))
}

fn read_u64(file: &mut File) -> Result<u64> {
    let mut bytes = [0u8; 8];
    file.read_exact(&mut bytes)?;
    Ok(u64::from_le_bytes(bytes))
}
