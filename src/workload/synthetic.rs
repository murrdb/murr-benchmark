use std::sync::Arc;

use rand::RngExt;
use serde::Deserialize;

use super::{DType, Field, Key, Request, Row, Schema, Value, Workload};

#[derive(Debug, Clone, Deserialize)]
pub struct SyntheticConfig {
    pub total_rows: usize,
    pub select_rows: usize,
    pub select_cols: usize,
}

/// Generated dataset: one utf8 `key` column (the row index as a string) and
/// `select_cols` random non-null Float32 columns `col_0..`. Requests sample
/// keys uniformly and ask for every column.
pub struct SyntheticWorkload {
    config: SyntheticConfig,
    schema: Arc<Schema>,
}

impl SyntheticWorkload {
    pub fn new(config: SyntheticConfig) -> Self {
        let schema = Schema {
            keys: vec![Field {
                name: "key".to_string(),
                dtype: DType::Utf8,
                nullable: false,
            }],
            values: (0..config.select_cols)
                .map(|i| Field {
                    name: format!("col_{i}"),
                    dtype: DType::Float32,
                    nullable: false,
                })
                .collect(),
        };
        SyntheticWorkload {
            config,
            schema: Arc::new(schema),
        }
    }
}

impl Workload for SyntheticWorkload {
    fn schema(&self) -> Arc<Schema> {
        self.schema.clone()
    }

    fn total_rows(&self) -> usize {
        self.config.total_rows
    }

    fn keys_per_request(&self) -> Option<usize> {
        Some(self.config.select_rows)
    }

    fn rows(&self) -> Box<dyn Iterator<Item = Row> + '_> {
        let num_cols = self.config.select_cols;
        let mut rng = rand::rng();
        Box::new((0..self.config.total_rows).map(move |i| Row {
            key: Key(vec![Value::Utf8(i.to_string())]),
            values: (0..num_cols)
                .map(|_| Some(Value::Float32(rng.random::<f32>())))
                .collect(),
        }))
    }

    /// Endless stream; every request draws fresh random keys in `[0, total_rows)`.
    fn requests(&self) -> Box<dyn Iterator<Item = Request> + '_> {
        let total_rows = self.config.total_rows;
        let select_rows = self.config.select_rows;
        let columns: Vec<String> = self.schema.values.iter().map(|f| f.name.clone()).collect();
        let mut rng = rand::rng();
        Box::new(std::iter::repeat_with(move || Request {
            keys: (0..select_rows)
                .map(|_| Key(vec![Value::Utf8(rng.random_range(0..total_rows).to_string())]))
                .collect(),
            columns: columns.clone(),
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn workload() -> SyntheticWorkload {
        SyntheticWorkload::new(SyntheticConfig {
            total_rows: 100,
            select_rows: 10,
            select_cols: 3,
        })
    }

    #[test]
    fn schema_is_one_utf8_key_and_float32_columns() {
        let schema = workload().schema();

        assert_eq!(schema.keys.len(), 1);
        assert_eq!(schema.keys[0].name, "key");
        assert_eq!(schema.keys[0].dtype, DType::Utf8);
        let names: Vec<&str> = schema.values.iter().map(|f| f.name.as_str()).collect();
        assert_eq!(names, ["col_0", "col_1", "col_2"]);
        assert!(schema.values.iter().all(|f| f.dtype == DType::Float32));
        assert!(schema.keys.iter().chain(&schema.values).all(|f| !f.nullable));
    }

    #[test]
    fn rows_are_keyed_by_index_and_have_no_nulls() {
        let rows: Vec<Row> = workload().rows().collect();

        assert_eq!(rows.len(), 100);
        assert_eq!(rows[0].key, Key(vec![Value::Utf8("0".to_string())]));
        assert_eq!(rows[99].key, Key(vec![Value::Utf8("99".to_string())]));
        assert!(rows.iter().all(|r| r.values.len() == 3));
        assert!(rows.iter().all(|r| r.values.iter().all(Option::is_some)));
    }

    #[test]
    fn requests_sample_existing_keys_and_all_columns() {
        let workload = workload();
        let requests: Vec<Request> = workload.requests().take(5).collect();

        assert_eq!(requests.len(), 5);
        for request in &requests {
            assert_eq!(request.keys.len(), 10);
            assert_eq!(request.columns, ["col_0", "col_1", "col_2"]);
            for key in &request.keys {
                let [Value::Utf8(index)] = key.0.as_slice() else {
                    panic!("expected a single utf8 key, got {key:?}");
                };
                assert!(index.parse::<usize>().unwrap() < 100);
            }
        }
    }
}
