use crate::common::{ColumnDef, Error, Result, Value};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DecodedRow {
    pub deleted: bool,
    pub values: Vec<Value>,
}

pub fn encode_row(values: &[Value], columns: &[ColumnDef]) -> Result<Vec<u8>> {
    if values.len() != columns.len() {
        return Err(Error::Storage("row column count mismatch".into()));
    }
    let mut buffer = Vec::new();
    buffer.push(0);
    buffer.extend_from_slice(&(values.len() as u16).to_le_bytes());
    for (value, column) in values.iter().zip(columns) {
        if value.value_type() != column.column_type {
            return Err(Error::Storage(format!(
                "type mismatch for column {}",
                column.name
            )));
        }
        match value {
            Value::Int(number) => buffer.extend_from_slice(&number.to_le_bytes()),
            Value::Text(text) => {
                let bytes = text.as_bytes();
                if bytes.len() > u16::MAX as usize {
                    return Err(Error::Storage("text value too large".into()));
                }
                buffer.extend_from_slice(&(bytes.len() as u16).to_le_bytes());
                buffer.extend_from_slice(bytes);
            }
        }
    }
    Ok(buffer)
}

pub fn decode_row(bytes: &[u8], columns: &[ColumnDef]) -> Result<DecodedRow> {
    if bytes.len() < 3 {
        return Err(Error::Storage("row payload too small".into()));
    }
    let deleted = bytes[0] != 0;
    let count = u16::from_le_bytes(bytes[1..3].try_into().unwrap()) as usize;
    if count != columns.len() {
        return Err(Error::Storage("row schema mismatch".into()));
    }
    let mut offset = 3usize;
    let mut values = Vec::with_capacity(columns.len());
    for column in columns {
        match column.column_type {
            crate::common::ColumnType::Int => {
                let end = offset + 8;
                let slice = bytes
                    .get(offset..end)
                    .ok_or_else(|| Error::Storage("row truncated while reading INT".into()))?;
                values.push(Value::Int(i64::from_le_bytes(slice.try_into().unwrap())));
                offset = end;
            }
            crate::common::ColumnType::Text => {
                let length_end = offset + 2;
                let len_slice = bytes.get(offset..length_end).ok_or_else(|| {
                    Error::Storage("row truncated while reading TEXT length".into())
                })?;
                let length = u16::from_le_bytes(len_slice.try_into().unwrap()) as usize;
                offset = length_end;
                let end = offset + length;
                let value_slice = bytes.get(offset..end).ok_or_else(|| {
                    Error::Storage("row truncated while reading TEXT payload".into())
                })?;
                let text = String::from_utf8(value_slice.to_vec())
                    .map_err(|_| Error::Storage("invalid utf-8 in row".into()))?;
                values.push(Value::Text(text));
                offset = end;
            }
        }
    }
    Ok(DecodedRow { deleted, values })
}
