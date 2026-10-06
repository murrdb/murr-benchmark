pub mod file;
pub mod synthetic;

use std::sync::Arc;

use serde::Deserialize;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum DType {
    Utf8,
    Float32,
    Float64,
    Int64,
}

#[derive(Debug, Clone, PartialEq)]
pub enum Value {
    Utf8(String),
    Float32(f32),
    Float64(f64),
    Int64(i64),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Field {
    pub name: String,
    pub dtype: DType,
    pub nullable: bool,
}

/// Backend-independent table schema. Key fields are listed in key order and
/// are never nullable; value fields may be.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Schema {
    pub keys: Vec<Field>,
    pub values: Vec<Field>,
}

/// One value per `Schema::keys` field. A key has no nulls.
#[derive(Debug, Clone, PartialEq)]
pub struct Key(pub Vec<Value>);

/// One table row. `values` follows `Schema::values`; `None` is a null.
#[derive(Debug, Clone, PartialEq)]
pub struct Row {
    pub key: Key,
    pub values: Vec<Option<Value>>,
}

/// Rows grouped by the harness for a single backend write.
#[derive(Debug, Clone, PartialEq)]
pub struct RowBatch {
    pub rows: Vec<Row>,
}

/// A single read: the keys to look up and the value columns to return.
#[derive(Debug, Clone, PartialEq)]
pub struct Request {
    pub keys: Vec<Key>,
    pub columns: Vec<String>,
}

/// A dataset plus the read traffic to run against it.
pub trait Workload {
    fn schema(&self) -> Arc<Schema>;

    /// Number of rows `rows()` yields.
    fn total_rows(&self) -> usize;

    /// Number of keys in each request, or `None` if it varies between requests.
    fn keys_per_request(&self) -> Option<usize>;

    fn rows(&self) -> Box<dyn Iterator<Item = Row> + '_>;

    fn requests(&self) -> Box<dyn Iterator<Item = Request> + '_>;
}
