use std::sync::Arc;

use arrow::array::{ArrayRef, Float32Array, Float64Array, Int64Array, StringArray};
use arrow::datatypes::{DataType, SchemaRef};
use arrow::record_batch::RecordBatch;

use crate::workload::{DType, Field, Key, RowBatch, Schema, Value};

use super::{BatchEncoder, KeySetEncoder};

impl From<DType> for DataType {
    fn from(dtype: DType) -> Self {
        match dtype {
            DType::Utf8 => DataType::Utf8,
            DType::Float32 => DataType::Float32,
            DType::Float64 => DataType::Float64,
            DType::Int64 => DataType::Int64,
        }
    }
}

impl From<&Field> for arrow::datatypes::Field {
    fn from(field: &Field) -> Self {
        arrow::datatypes::Field::new(&field.name, field.dtype.into(), field.nullable)
    }
}

/// Encodes a write batch as an Arrow `RecordBatch`: key columns first, then value columns.
#[derive(Clone)]
pub struct ArrowBatch {
    schema: Arc<Schema>,
    arrow_schema: SchemaRef,
}

impl ArrowBatch {
    pub fn new(schema: Arc<Schema>) -> Self {
        let fields: Vec<arrow::datatypes::Field> = schema
            .keys
            .iter()
            .chain(&schema.values)
            .map(Into::into)
            .collect();
        ArrowBatch {
            schema,
            arrow_schema: Arc::new(arrow::datatypes::Schema::new(fields)),
        }
    }
}

impl BatchEncoder for ArrowBatch {
    type Output = RecordBatch;

    fn encode(&self, batch: &RowBatch) -> RecordBatch {
        let keys = self.schema.keys.iter().enumerate().map(|(i, field)| {
            column(field, batch.rows.iter().map(move |row| Some(&row.key.0[i])))
        });
        let values = self.schema.values.iter().enumerate().map(|(i, field)| {
            column(field, batch.rows.iter().map(move |row| row.values[i].as_ref()))
        });
        let columns: Vec<ArrayRef> = keys.chain(values).collect();
        RecordBatch::try_new(self.arrow_schema.clone(), columns)
            .unwrap_or_else(|e| panic!("failed to create RecordBatch: {e}"))
    }
}

/// Encodes request keys as an Arrow `RecordBatch` with one column per key field.
#[derive(Clone)]
pub struct ArrowKeys {
    schema: Arc<Schema>,
    arrow_schema: SchemaRef,
}

impl ArrowKeys {
    pub fn new(schema: Arc<Schema>) -> Self {
        let fields: Vec<arrow::datatypes::Field> = schema.keys.iter().map(Into::into).collect();
        ArrowKeys {
            schema,
            arrow_schema: Arc::new(arrow::datatypes::Schema::new(fields)),
        }
    }
}

impl KeySetEncoder for ArrowKeys {
    type Output = RecordBatch;

    fn encode(&self, keys: &[Key]) -> RecordBatch {
        let columns: Vec<ArrayRef> = self
            .schema
            .keys
            .iter()
            .enumerate()
            .map(|(i, field)| column(field, keys.iter().map(move |key| Some(&key.0[i]))))
            .collect();
        RecordBatch::try_new(self.arrow_schema.clone(), columns)
            .unwrap_or_else(|e| panic!("failed to create keys RecordBatch: {e}"))
    }
}

fn column<'a>(field: &Field, values: impl Iterator<Item = Option<&'a Value>>) -> ArrayRef {
    match field.dtype {
        DType::Utf8 => Arc::new(
            values
                .map(|value| {
                    value.map(|value| match value {
                        Value::Utf8(s) => s.as_str(),
                        other => panic!("column {}: expected utf8, got {other:?}", field.name),
                    })
                })
                .collect::<StringArray>(),
        ),
        DType::Float32 => Arc::new(
            values
                .map(|value| {
                    value.map(|value| match value {
                        Value::Float32(v) => *v,
                        other => panic!("column {}: expected float32, got {other:?}", field.name),
                    })
                })
                .collect::<Float32Array>(),
        ),
        DType::Float64 => Arc::new(
            values
                .map(|value| {
                    value.map(|value| match value {
                        Value::Float64(v) => *v,
                        other => panic!("column {}: expected float64, got {other:?}", field.name),
                    })
                })
                .collect::<Float64Array>(),
        ),
        DType::Int64 => Arc::new(
            values
                .map(|value| {
                    value.map(|value| match value {
                        Value::Int64(v) => *v,
                        other => panic!("column {}: expected int64, got {other:?}", field.name),
                    })
                })
                .collect::<Int64Array>(),
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::workload::Row;
    use arrow::array::{Array, AsArray};
    use arrow::datatypes::{Float32Type, Float64Type, Int64Type};

    fn schema() -> Arc<Schema> {
        Arc::new(Schema {
            keys: vec![Field {
                name: "key".to_string(),
                dtype: DType::Utf8,
                nullable: false,
            }],
            values: vec![
                Field {
                    name: "a".to_string(),
                    dtype: DType::Float32,
                    nullable: false,
                },
                Field {
                    name: "b".to_string(),
                    dtype: DType::Float32,
                    nullable: true,
                },
            ],
        })
    }

    fn key(value: &str) -> Key {
        Key(vec![Value::Utf8(value.to_string())])
    }

    #[test]
    fn batch_has_keys_first_and_keeps_nulls() {
        let batch = RowBatch {
            rows: vec![
                Row {
                    key: key("x"),
                    values: vec![Some(Value::Float32(1.0)), Some(Value::Float32(2.0))],
                },
                Row {
                    key: key("y"),
                    values: vec![Some(Value::Float32(3.0)), None],
                },
            ],
        };

        let encoded = ArrowBatch::new(schema()).encode(&batch);

        let schema = encoded.schema();
        let names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
        assert_eq!(names, ["key", "a", "b"]);
        let nullable: Vec<bool> = schema.fields().iter().map(|f| f.is_nullable()).collect();
        assert_eq!(nullable, [false, false, true]);
        let keys: Vec<&str> = encoded.column(0).as_string::<i32>().iter().flatten().collect();
        assert_eq!(keys, ["x", "y"]);
        assert_eq!(encoded.column(1).as_primitive::<Float32Type>().values(), &[1.0, 3.0]);
        let b = encoded.column(2).as_primitive::<Float32Type>();
        assert_eq!(b.value(0), 2.0);
        assert!(b.is_null(1));
    }

    #[test]
    fn keys_have_one_column_per_key_field() {
        let encoded = ArrowKeys::new(schema()).encode(&[key("x"), key("y")]);

        assert_eq!(encoded.num_columns(), 1);
        assert_eq!(encoded.schema().field(0).name(), "key");
        let keys: Vec<&str> = encoded.column(0).as_string::<i32>().iter().flatten().collect();
        assert_eq!(keys, ["x", "y"]);
    }

    #[test]
    fn wide_numeric_columns_keep_type_and_nulls() {
        let field = |name: &str, dtype| Field {
            name: name.to_string(),
            dtype,
            nullable: true,
        };
        let schema = Arc::new(Schema {
            keys: schema().keys.clone(),
            values: vec![field("f", DType::Float64), field("i", DType::Int64)],
        });
        let batch = RowBatch {
            rows: vec![
                Row {
                    key: key("x"),
                    values: vec![Some(Value::Float64(1.5)), None],
                },
                Row {
                    key: key("y"),
                    values: vec![None, Some(Value::Int64(7))],
                },
            ],
        };

        let encoded = ArrowBatch::new(schema).encode(&batch);

        let f: Vec<Option<f64>> = encoded.column(1).as_primitive::<Float64Type>().iter().collect();
        assert_eq!(f, [Some(1.5), None]);
        let i: Vec<Option<i64>> = encoded.column(2).as_primitive::<Int64Type>().iter().collect();
        assert_eq!(i, [None, Some(7)]);
    }
}
