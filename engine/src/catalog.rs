use crate::common::{ColumnDef, ColumnType, Error, Result};

#[derive(Debug, Clone)]
pub struct Catalog {
    pub next_table_id: u32,
    pub next_index_id: u32,
    pub tables: Vec<TableMeta>,
    pub indexes: Vec<IndexMeta>,
}

#[derive(Debug, Clone)]
pub struct TableMeta {
    pub id: u32,
    pub name: String,
    pub columns: Vec<ColumnDef>,
    pub first_heap_page: u32,
    pub last_heap_page: u32,
}

#[derive(Debug, Clone)]
pub struct IndexMeta {
    pub id: u32,
    pub name: String,
    pub table_id: u32,
    pub column_name: String,
    pub root_page_id: u32,
    pub key_type: ColumnType,
}

impl Catalog {
    pub fn new() -> Self {
        Self {
            next_table_id: 1,
            next_index_id: 1,
            tables: Vec::new(),
            indexes: Vec::new(),
        }
    }

    pub fn table(&self, name: &str) -> Option<&TableMeta> {
        self.tables.iter().find(|table| table.name.eq_ignore_ascii_case(name))
    }

    pub fn table_mut(&mut self, name: &str) -> Option<&mut TableMeta> {
        self.tables
            .iter_mut()
            .find(|table| table.name.eq_ignore_ascii_case(name))
    }

    pub fn table_by_id(&self, table_id: u32) -> Option<&TableMeta> {
        self.tables.iter().find(|table| table.id == table_id)
    }

    pub fn table_by_id_mut(&mut self, table_id: u32) -> Option<&mut TableMeta> {
        self.tables.iter_mut().find(|table| table.id == table_id)
    }

    pub fn index_for_column(&self, table_id: u32, column: &str) -> Option<&IndexMeta> {
        self.indexes
            .iter()
            .find(|index| index.table_id == table_id && index.column_name.eq_ignore_ascii_case(column))
    }

    pub fn indexes_for_table(&self, table_id: u32) -> Vec<IndexMeta> {
        self.indexes
            .iter()
            .filter(|index| index.table_id == table_id)
            .cloned()
            .collect()
    }

    pub fn serialize(&self) -> Result<Vec<u8>> {
        let mut buffer = Vec::new();
        write_u32(&mut buffer, self.next_table_id);
        write_u32(&mut buffer, self.next_index_id);
        write_u32(&mut buffer, self.tables.len() as u32);
        write_u32(&mut buffer, self.indexes.len() as u32);
        for table in &self.tables {
            write_u32(&mut buffer, table.id);
            write_string(&mut buffer, &table.name)?;
            write_u16(&mut buffer, table.columns.len() as u16);
            for column in &table.columns {
                write_string(&mut buffer, &column.name)?;
                buffer.push(column.column_type.to_tag());
            }
            write_u32(&mut buffer, table.first_heap_page);
            write_u32(&mut buffer, table.last_heap_page);
        }
        for index in &self.indexes {
            write_u32(&mut buffer, index.id);
            write_string(&mut buffer, &index.name)?;
            write_u32(&mut buffer, index.table_id);
            write_string(&mut buffer, &index.column_name)?;
            write_u32(&mut buffer, index.root_page_id);
            buffer.push(index.key_type.to_tag());
        }
        Ok(buffer)
    }

    pub fn deserialize(bytes: &[u8]) -> Result<Self> {
        if bytes.is_empty() {
            return Ok(Self::new());
        }
        let mut cursor = Cursor::new(bytes);
        let next_table_id = cursor.read_u32()?;
        let next_index_id = cursor.read_u32()?;
        let table_count = cursor.read_u32()? as usize;
        let index_count = cursor.read_u32()? as usize;
        let mut tables = Vec::with_capacity(table_count);
        let mut indexes = Vec::with_capacity(index_count);
        for _ in 0..table_count {
            let id = cursor.read_u32()?;
            let name = cursor.read_string()?;
            let column_count = cursor.read_u16()? as usize;
            let mut columns = Vec::with_capacity(column_count);
            for _ in 0..column_count {
                let column_name = cursor.read_string()?;
                let column_type = ColumnType::from_tag(cursor.read_u8()?)?;
                columns.push(ColumnDef {
                    name: column_name,
                    column_type,
                });
            }
            let first_heap_page = cursor.read_u32()?;
            let last_heap_page = cursor.read_u32()?;
            tables.push(TableMeta {
                id,
                name,
                columns,
                first_heap_page,
                last_heap_page,
            });
        }
        for _ in 0..index_count {
            indexes.push(IndexMeta {
                id: cursor.read_u32()?,
                name: cursor.read_string()?,
                table_id: cursor.read_u32()?,
                column_name: cursor.read_string()?,
                root_page_id: cursor.read_u32()?,
                key_type: ColumnType::from_tag(cursor.read_u8()?)?,
            });
        }
        Ok(Self {
            next_table_id,
            next_index_id,
            tables,
            indexes,
        })
    }
}

fn write_u16(buffer: &mut Vec<u8>, value: u16) {
    buffer.extend_from_slice(&value.to_le_bytes());
}

fn write_u32(buffer: &mut Vec<u8>, value: u32) {
    buffer.extend_from_slice(&value.to_le_bytes());
}

fn write_string(buffer: &mut Vec<u8>, value: &str) -> Result<()> {
    let bytes = value.as_bytes();
    if bytes.len() > u16::MAX as usize {
        return Err(Error::Catalog("string too large to serialize".into()));
    }
    write_u16(buffer, bytes.len() as u16);
    buffer.extend_from_slice(bytes);
    Ok(())
}

struct Cursor<'a> {
    bytes: &'a [u8],
    index: usize,
}

impl<'a> Cursor<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, index: 0 }
    }

    fn read_u8(&mut self) -> Result<u8> {
        let value = *self
            .bytes
            .get(self.index)
            .ok_or_else(|| Error::Catalog("unexpected end of catalog".into()))?;
        self.index += 1;
        Ok(value)
    }

    fn read_u16(&mut self) -> Result<u16> {
        let end = self.index + 2;
        let slice = self
            .bytes
            .get(self.index..end)
            .ok_or_else(|| Error::Catalog("unexpected end of catalog".into()))?;
        self.index = end;
        Ok(u16::from_le_bytes([slice[0], slice[1]]))
    }

    fn read_u32(&mut self) -> Result<u32> {
        let end = self.index + 4;
        let slice = self
            .bytes
            .get(self.index..end)
            .ok_or_else(|| Error::Catalog("unexpected end of catalog".into()))?;
        self.index = end;
        Ok(u32::from_le_bytes([slice[0], slice[1], slice[2], slice[3]]))
    }

    fn read_string(&mut self) -> Result<String> {
        let length = self.read_u16()? as usize;
        let end = self.index + length;
        let slice = self
            .bytes
            .get(self.index..end)
            .ok_or_else(|| Error::Catalog("unexpected end of catalog".into()))?;
        self.index = end;
        String::from_utf8(slice.to_vec())
            .map_err(|_| Error::Catalog("invalid utf-8 in catalog".into()))
    }
}
