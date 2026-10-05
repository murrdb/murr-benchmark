use crate::workload::{Row, Schema, Value};

use super::{RowEncoder, ValueEncoder};

/// Native-width little-endian bytes for numbers, raw bytes for strings.
#[derive(Debug, Clone, Copy)]
pub struct LittleEndian;

impl ValueEncoder for LittleEndian {
    fn encode(&self, value: &Value, buf: &mut Vec<u8>) {
        match value {
            Value::Utf8(s) => buf.extend_from_slice(s.as_bytes()),
            Value::Float32(v) => buf.extend_from_slice(&v.to_le_bytes()),
        }
    }
}

/// Packs a row's values into one blob, in `Schema::values` order.
///
/// Layout: an optional null bitmap, then each non-null value as `LittleEndian`
/// bytes; strings are prefixed with their byte length as a `u32`.
///
/// The bitmap (one bit per value field, LSB first, set when the value is
/// present) is written only if the schema has a nullable value field. A null
/// value contributes no bytes after the bitmap.
#[derive(Debug, Clone, Copy)]
pub struct BlobRow {
    bitmap_len: usize,
}

impl BlobRow {
    pub fn new(schema: &Schema) -> Self {
        let has_nullable = schema.values.iter().any(|f| f.nullable);
        BlobRow {
            bitmap_len: if has_nullable {
                schema.values.len().div_ceil(8)
            } else {
                0
            },
        }
    }
}

impl RowEncoder for BlobRow {
    fn encode(&self, row: &Row, buf: &mut Vec<u8>) {
        let bitmap_start = buf.len();
        buf.resize(bitmap_start + self.bitmap_len, 0);
        for (i, value) in row.values.iter().enumerate() {
            let Some(value) = value else {
                assert!(self.bitmap_len > 0, "null value in a schema without nullable fields");
                continue;
            };
            if self.bitmap_len > 0 {
                buf[bitmap_start + i / 8] |= 1 << (i % 8);
            }
            if let Value::Utf8(s) = value {
                buf.extend_from_slice(&(s.len() as u32).to_le_bytes());
            }
            LittleEndian.encode(value, buf);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::workload::{DType, Field, Key};

    fn schema(fields: &[(DType, bool)]) -> Schema {
        Schema {
            keys: vec![Field {
                name: "key".to_string(),
                dtype: DType::Utf8,
                nullable: false,
            }],
            values: fields
                .iter()
                .enumerate()
                .map(|(i, (dtype, nullable))| Field {
                    name: format!("v{i}"),
                    dtype: *dtype,
                    nullable: *nullable,
                })
                .collect(),
        }
    }

    fn row(values: Vec<Option<Value>>) -> Row {
        Row {
            key: Key(vec![Value::Utf8("k".to_string())]),
            values,
        }
    }

    #[test]
    fn non_nullable_schema_is_plain_concatenated_values() {
        let schema = schema(&[(DType::Float32, false), (DType::Float32, false)]);
        let row = row(vec![Some(Value::Float32(1.0)), Some(Value::Float32(2.0))]);
        let mut buf = Vec::new();

        BlobRow::new(&schema).encode(&row, &mut buf);

        let mut expected = 1.0f32.to_le_bytes().to_vec();
        expected.extend_from_slice(&2.0f32.to_le_bytes());
        assert_eq!(buf, expected);
    }

    #[test]
    fn nullable_schema_prepends_bitmap_and_skips_nulls() {
        let schema = schema(&[
            (DType::Float32, true),
            (DType::Float32, true),
            (DType::Float32, false),
        ]);
        let row = row(vec![None, Some(Value::Float32(2.0)), Some(Value::Float32(3.0))]);
        let mut buf = Vec::new();

        BlobRow::new(&schema).encode(&row, &mut buf);

        let mut expected = vec![0b110u8];
        expected.extend_from_slice(&2.0f32.to_le_bytes());
        expected.extend_from_slice(&3.0f32.to_le_bytes());
        assert_eq!(buf, expected);
    }

    #[test]
    fn strings_are_length_prefixed() {
        let schema = schema(&[(DType::Utf8, false)]);
        let row = row(vec![Some(Value::Utf8("abc".to_string()))]);
        let mut buf = Vec::new();

        BlobRow::new(&schema).encode(&row, &mut buf);

        assert_eq!(buf, [3, 0, 0, 0, b'a', b'b', b'c']);
    }

    #[test]
    fn appends_to_existing_buffer() {
        let schema = schema(&[(DType::Float32, true)]);
        let row = row(vec![Some(Value::Float32(1.0))]);
        let mut buf = vec![0xff];

        BlobRow::new(&schema).encode(&row, &mut buf);

        let mut expected = vec![0xff, 0b1];
        expected.extend_from_slice(&1.0f32.to_le_bytes());
        assert_eq!(buf, expected);
    }
}
