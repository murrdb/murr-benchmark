use indexmap::IndexMap;
use murr::core::{ColumnSchema, DTypeName, TableSchema};

use crate::workload::{DType, Schema};

impl From<DType> for DTypeName {
    fn from(dtype: DType) -> Self {
        match dtype {
            DType::Utf8 => DTypeName::Utf8,
            DType::Float32 => DTypeName::Float32,
        }
    }
}

/// Key columns first, in key order, which is the order murr encodes key bytes in.
impl From<&Schema> for TableSchema {
    fn from(schema: &Schema) -> Self {
        let keys = schema.keys.iter().map(|f| (f, true));
        let values = schema.values.iter().map(|f| (f, false));
        let columns: IndexMap<String, ColumnSchema> = keys
            .chain(values)
            .map(|(field, key)| {
                (
                    field.name.clone(),
                    ColumnSchema {
                        dtype: field.dtype.into(),
                        nullable: field.nullable,
                        key,
                        strict: true,
                    },
                )
            })
            .collect();
        TableSchema { columns }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::workload::Field;

    #[test]
    fn table_schema_marks_keys_and_keeps_order() {
        let schema = Schema {
            keys: vec![Field {
                name: "key".to_string(),
                dtype: DType::Utf8,
                nullable: false,
            }],
            values: vec![Field {
                name: "col_0".to_string(),
                dtype: DType::Float32,
                nullable: true,
            }],
        };

        let table = TableSchema::from(&schema);

        let names: Vec<&String> = table.columns.keys().collect();
        assert_eq!(names, ["key", "col_0"]);
        let key = &table.columns["key"];
        assert_eq!(key.dtype, DTypeName::Utf8);
        assert!(key.key);
        assert!(!key.nullable);
        let value = &table.columns["col_0"];
        assert_eq!(value.dtype, DTypeName::Float32);
        assert!(!value.key);
        assert!(value.nullable);
    }
}
