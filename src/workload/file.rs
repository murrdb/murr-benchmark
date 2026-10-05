use std::collections::HashMap;
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use arrow::array::{ArrayRef, AsArray};
use arrow::datatypes::{DataType, Float32Type, Float64Type, Int64Type};
use arrow::record_batch::RecordBatch;
use flate2::read::MultiGzDecoder;
use parquet::arrow::ProjectionMask;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use serde::Deserialize;

use super::{DType, Field, Key, Request, Row, Schema, Value, Workload};

#[derive(Debug, Clone, Deserialize)]
pub struct FieldConfig {
    pub name: String,
    pub dtype: DType,
    /// Defaults to `true` for value fields. Key fields are never nullable.
    pub nullable: Option<bool>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct SchemaConfig {
    /// In key order.
    pub keys: Vec<FieldConfig>,
    pub values: Vec<FieldConfig>,
}

impl From<&SchemaConfig> for Schema {
    fn from(config: &SchemaConfig) -> Self {
        Schema {
            keys: config
                .keys
                .iter()
                .map(|f| Field {
                    name: f.name.clone(),
                    dtype: f.dtype,
                    nullable: false,
                })
                .collect(),
            values: config
                .values
                .iter()
                .map(|f| Field {
                    name: f.name.clone(),
                    dtype: f.dtype,
                    nullable: f.nullable.unwrap_or(true),
                })
                .collect(),
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct FileConfig {
    /// Directory of `*.parquet` files holding the table rows.
    pub rows: PathBuf,
    /// Request log, one JSON object per line; gzip-compressed if the name ends in `.gz`.
    pub requests: PathBuf,
    pub schema: SchemaConfig,
}

/// One line of the request log. `keys` holds parallel arrays named by key
/// field; other fields of the line are ignored.
#[derive(Debug, Deserialize)]
struct LoggedRequest {
    keys: HashMap<String, Vec<String>>,
    features: Vec<String>,
}

/// A recorded dataset: rows come from Parquet files, restricted to the columns
/// of the configured schema, and requests are replayed from a log in file
/// order, starting over when the log ends.
pub struct FileWorkload {
    schema: Arc<Schema>,
    files: Vec<PathBuf>,
    requests: PathBuf,
    total_rows: usize,
}

impl FileWorkload {
    pub fn new(config: FileConfig) -> Self {
        for key in &config.schema.keys {
            assert!(
                key.nullable != Some(true),
                "key field {} cannot be nullable",
                key.name
            );
            // The request log carries key values as strings.
            assert!(
                key.dtype == DType::Utf8,
                "key field {} must be utf8, got {:?}",
                key.name,
                key.dtype
            );
        }
        let schema = Schema::from(&config.schema);

        let mut files: Vec<PathBuf> = std::fs::read_dir(&config.rows)
            .unwrap_or_else(|e| panic!("failed to list {}: {e}", config.rows.display()))
            .map(|entry| entry.expect("failed to read directory entry").path())
            .filter(|path| path.extension().is_some_and(|ext| ext == "parquet"))
            .collect();
        files.sort();
        assert!(
            !files.is_empty(),
            "no parquet files in {}",
            config.rows.display()
        );

        let mut total_rows = 0;
        for path in &files {
            let builder = open_parquet(path);
            for field in schema.keys.iter().chain(&schema.values) {
                let column = builder.schema().field_with_name(&field.name).unwrap_or_else(|_| {
                    panic!("{}: no column {}", path.display(), field.name)
                });
                let expected = DataType::from(field.dtype);
                assert!(
                    column.data_type() == &expected,
                    "{}: column {} is {}, schema says {expected}",
                    path.display(),
                    field.name,
                    column.data_type()
                );
            }
            total_rows += builder.metadata().file_metadata().num_rows() as usize;
        }

        FileWorkload {
            schema: Arc::new(schema),
            files,
            requests: config.requests,
            total_rows,
        }
    }

    fn batch_rows(&self, path: &Path, batch: &RecordBatch) -> Vec<Row> {
        let columns = |fields: &[Field]| -> Vec<std::vec::IntoIter<Option<Value>>> {
            fields
                .iter()
                .map(|field| {
                    let array = batch
                        .column_by_name(&field.name)
                        .unwrap_or_else(|| panic!("{}: no column {}", path.display(), field.name));
                    column_values(array, field.dtype).into_iter()
                })
                .collect()
        };
        let mut keys = columns(&self.schema.keys);
        let mut values = columns(&self.schema.values);

        (0..batch.num_rows())
            .map(|_| Row {
                key: Key(keys
                    .iter_mut()
                    .zip(&self.schema.keys)
                    .map(|(column, field)| {
                        column.next().flatten().unwrap_or_else(|| {
                            panic!("{}: null in key column {}", path.display(), field.name)
                        })
                    })
                    .collect()),
                values: values.iter_mut().map(|column| column.next().flatten()).collect(),
            })
            .collect()
    }

    fn request(&self, line: &str) -> Request {
        let mut logged: LoggedRequest = serde_json::from_str(line)
            .unwrap_or_else(|e| panic!("{}: bad request line: {e}", self.requests.display()));
        let mut columns: Vec<std::vec::IntoIter<String>> = self
            .schema
            .keys
            .iter()
            .map(|field| {
                logged
                    .keys
                    .remove(&field.name)
                    .unwrap_or_else(|| {
                        panic!("{}: request has no key {}", self.requests.display(), field.name)
                    })
                    .into_iter()
            })
            .collect();
        let num_keys = columns[0].len();
        assert!(
            columns.iter().all(|column| column.len() == num_keys),
            "{}: request key arrays differ in length",
            self.requests.display()
        );

        Request {
            keys: (0..num_keys)
                .map(|_| {
                    Key(columns
                        .iter_mut()
                        .map(|column| Value::Utf8(column.next().expect("length checked above")))
                        .collect())
                })
                .collect(),
            columns: logged.features,
        }
    }
}

fn open_parquet(path: &Path) -> ParquetRecordBatchReaderBuilder<File> {
    let file =
        File::open(path).unwrap_or_else(|e| panic!("failed to open {}: {e}", path.display()));
    ParquetRecordBatchReaderBuilder::try_new(file)
        .unwrap_or_else(|e| panic!("failed to read {}: {e}", path.display()))
}

fn column_values(array: &ArrayRef, dtype: DType) -> Vec<Option<Value>> {
    match dtype {
        DType::Utf8 => array
            .as_string::<i32>()
            .iter()
            .map(|v| v.map(|s| Value::Utf8(s.to_string())))
            .collect(),
        DType::Float32 => array
            .as_primitive::<Float32Type>()
            .iter()
            .map(|v| v.map(Value::Float32))
            .collect(),
        DType::Float64 => array
            .as_primitive::<Float64Type>()
            .iter()
            .map(|v| v.map(Value::Float64))
            .collect(),
        DType::Int64 => array
            .as_primitive::<Int64Type>()
            .iter()
            .map(|v| v.map(Value::Int64))
            .collect(),
    }
}

/// Non-empty lines of a text file, transparently gunzipped for `.gz` paths.
/// The decoder handles files made of several concatenated gzip members.
fn lines(path: &Path) -> impl Iterator<Item = String> + use<> {
    let file =
        File::open(path).unwrap_or_else(|e| panic!("failed to open {}: {e}", path.display()));
    let reader: Box<dyn BufRead> = if path.extension().is_some_and(|ext| ext == "gz") {
        Box::new(BufReader::new(MultiGzDecoder::new(file)))
    } else {
        Box::new(BufReader::new(file))
    };
    let path = path.to_path_buf();
    reader
        .lines()
        .map(move |line| line.unwrap_or_else(|e| panic!("failed to read {}: {e}", path.display())))
        .filter(|line| !line.trim().is_empty())
}

impl Workload for FileWorkload {
    fn schema(&self) -> Arc<Schema> {
        self.schema.clone()
    }

    fn total_rows(&self) -> usize {
        self.total_rows
    }

    /// Logged requests carry a varying number of keys.
    fn keys_per_request(&self) -> Option<usize> {
        None
    }

    fn rows(&self) -> Box<dyn Iterator<Item = Row> + '_> {
        let names: Vec<&str> = self
            .schema
            .keys
            .iter()
            .chain(&self.schema.values)
            .map(|f| f.name.as_str())
            .collect();
        Box::new(self.files.iter().flat_map(move |path| {
            let builder = open_parquet(path);
            let projection = ProjectionMask::columns(builder.parquet_schema(), names.iter().copied());
            let reader = builder
                .with_projection(projection)
                .build()
                .unwrap_or_else(|e| panic!("failed to read {}: {e}", path.display()));
            reader.flat_map(move |batch| {
                let batch =
                    batch.unwrap_or_else(|e| panic!("failed to read {}: {e}", path.display()));
                self.batch_rows(path, &batch)
            })
        }))
    }

    /// Endless stream: the log is replayed from the start whenever it ends.
    fn requests(&self) -> Box<dyn Iterator<Item = Request> + '_> {
        let passes = std::iter::repeat_with(move || {
            let mut pass = lines(&self.requests).peekable();
            assert!(
                pass.peek().is_some(),
                "{}: request log is empty",
                self.requests.display()
            );
            pass
        });
        Box::new(passes.flatten().map(move |line| self.request(&line)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Float64Array, Int64Array, StringArray};
    use flate2::Compression;
    use flate2::write::GzEncoder;
    use parquet::arrow::ArrowWriter;
    use std::io::Write;
    use tempfile::TempDir;

    const LOG: &str = concat!(
        r#"{"request_id":"r1","ts":"t1","keys":{"geid":["A","A"],"vendor":["v1","v2"]},"features":["price"]}"#,
        "\n",
        r#"{"request_id":"r2","ts":"t2","keys":{"vendor":["v3"],"geid":["B"]},"features":["price","orders"]}"#,
        "\n",
    );

    fn write_parquet(path: &Path, geid: &[&str], vendor: &[&str], price: &[Option<f64>]) {
        let orders: Vec<Option<i64>> = (0..geid.len() as i64).map(Some).collect();
        let names: Vec<Option<&str>> = geid.iter().map(|_| None).collect();
        let batch = RecordBatch::try_from_iter_with_nullable([
            ("geid", Arc::new(StringArray::from(geid.to_vec())) as ArrayRef, true),
            ("vendor", Arc::new(StringArray::from(vendor.to_vec())) as ArrayRef, true),
            ("unlisted", Arc::new(Int64Array::from(orders.clone())) as ArrayRef, true),
            ("price", Arc::new(Float64Array::from(price.to_vec())) as ArrayRef, true),
            ("orders", Arc::new(Int64Array::from(orders)) as ArrayRef, true),
            ("name", Arc::new(StringArray::from(names)) as ArrayRef, true),
        ])
        .unwrap();
        let mut writer = ArrowWriter::try_new(File::create(path).unwrap(), batch.schema(), None).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
    }

    /// Two Parquet parts (3 rows) and a two-line request log.
    fn fixture(log_name: &str, log: &[u8]) -> (TempDir, FileConfig) {
        let dir = TempDir::new().unwrap();
        let rows = dir.path().join("rows");
        std::fs::create_dir(&rows).unwrap();
        write_parquet(&rows.join("part-1.parquet"), &["B"], &["v3"], &[Some(3.5)]);
        write_parquet(
            &rows.join("part-0.parquet"),
            &["A", "A"],
            &["v1", "v2"],
            &[Some(1.5), None],
        );
        let requests = dir.path().join(log_name);
        std::fs::write(&requests, log).unwrap();

        let field = |name: &str, dtype| FieldConfig {
            name: name.to_string(),
            dtype,
            nullable: None,
        };
        let config = FileConfig {
            rows,
            requests,
            schema: SchemaConfig {
                keys: vec![field("geid", DType::Utf8), field("vendor", DType::Utf8)],
                values: vec![
                    field("price", DType::Float64),
                    field("orders", DType::Int64),
                    field("name", DType::Utf8),
                ],
            },
        };
        (dir, config)
    }

    fn key(geid: &str, vendor: &str) -> Key {
        Key(vec![Value::Utf8(geid.to_string()), Value::Utf8(vendor.to_string())])
    }

    #[test]
    fn schema_has_non_null_keys_and_nullable_values() {
        let (_dir, config) = fixture("requests.jsonl", LOG.as_bytes());

        let schema = FileWorkload::new(config).schema();

        assert!(schema.keys.iter().all(|f| !f.nullable));
        assert!(schema.values.iter().all(|f| f.nullable));
    }

    #[test]
    fn rows_come_from_all_parts_in_name_order_with_nulls() {
        let (_dir, config) = fixture("requests.jsonl", LOG.as_bytes());
        let workload = FileWorkload::new(config);

        let rows: Vec<Row> = workload.rows().collect();

        assert_eq!(workload.total_rows(), 3);
        assert_eq!(
            rows,
            vec![
                Row {
                    key: key("A", "v1"),
                    values: vec![Some(Value::Float64(1.5)), Some(Value::Int64(0)), None],
                },
                Row {
                    key: key("A", "v2"),
                    values: vec![None, Some(Value::Int64(1)), None],
                },
                Row {
                    key: key("B", "v3"),
                    values: vec![Some(Value::Float64(3.5)), Some(Value::Int64(0)), None],
                },
            ]
        );
    }

    #[test]
    fn requests_follow_schema_key_order_and_loop() {
        let (_dir, config) = fixture("requests.jsonl", LOG.as_bytes());
        let workload = FileWorkload::new(config);

        let requests: Vec<Request> = workload.requests().take(3).collect();

        assert_eq!(workload.keys_per_request(), None);
        assert_eq!(requests[0].keys, vec![key("A", "v1"), key("A", "v2")]);
        assert_eq!(requests[0].columns, ["price"]);
        assert_eq!(requests[1].keys, vec![key("B", "v3")]);
        assert_eq!(requests[1].columns, ["price", "orders"]);
        assert_eq!(requests[2], requests[0]);
    }

    #[test]
    fn gzipped_log_with_several_members_is_read() {
        let mut gz = Vec::new();
        for line in LOG.lines() {
            let mut encoder = GzEncoder::new(&mut gz, Compression::default());
            writeln!(encoder, "{line}").unwrap();
            encoder.finish().unwrap();
        }
        let (_dir, config) = fixture("requests.jsonl.gz", &gz);
        let workload = FileWorkload::new(config);

        let requests: Vec<Request> = workload.requests().take(2).collect();

        assert_eq!(requests[0].keys.len(), 2);
        assert_eq!(requests[1].keys, vec![key("B", "v3")]);
    }

    #[test]
    #[should_panic(expected = "column price is Float64, schema says Int64")]
    fn column_type_mismatch_is_rejected() {
        let (_dir, mut config) = fixture("requests.jsonl", LOG.as_bytes());
        config.schema.values[0].dtype = DType::Int64;

        FileWorkload::new(config);
    }

    #[test]
    #[should_panic(expected = "key field geid cannot be nullable")]
    fn nullable_key_is_rejected() {
        let (_dir, mut config) = fixture("requests.jsonl", LOG.as_bytes());
        config.schema.keys[0].nullable = Some(true);

        FileWorkload::new(config);
    }
}
