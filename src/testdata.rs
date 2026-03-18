use std::sync::Arc;

use arrow::array::{Float32Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use rand::RngExt;

/// Column names: `["col_0", "col_1", ..., "col_{n-1}"]`.
pub fn column_names(num_cols: usize) -> Vec<String> {
    (0..num_cols).map(|i| format!("col_{i}")).collect()
}

/// Build an Arrow schema: 1 Utf8 "key" column + `num_cols` Float32 columns.
pub fn make_schema(num_cols: usize) -> Schema {
    let mut fields = vec![Field::new("key", DataType::Utf8, false)];
    for name in column_names(num_cols) {
        fields.push(Field::new(name, DataType::Float32, false));
    }
    Schema::new(fields)
}

/// Generate `count` random key strings in `[0, max_key)` with a fresh random seed.
/// Each call produces a different set of keys.
pub fn generate_random_keys(count: usize, max_key: usize) -> Vec<String> {
    let mut rng = rand::rng();
    (0..count)
        .map(|_| rng.random_range(0..max_key).to_string())
        .collect()
}

/// Iterator yielding RecordBatches of `batch_size` rows, totaling `total_rows`.
///
/// Key column: row index as string (`"0"`, `"1"`, ...).
/// Float32 columns: row index cast to f32.
pub fn generate_batches(
    schema: &Schema,
    total_rows: usize,
    batch_size: usize,
) -> impl Iterator<Item = RecordBatch> + '_ {
    let num_cols = schema.fields().len() - 1; // exclude "key"
    let num_batches = total_rows.div_ceil(batch_size);

    (0..num_batches).map(move |batch_idx| {
        let start = batch_idx * batch_size;
        let end = (start + batch_size).min(total_rows);
        let len = end - start;

        let keys: StringArray = (start..end).map(|i| Some(i.to_string())).collect();

        let mut columns: Vec<Arc<dyn arrow::array::Array>> = Vec::with_capacity(num_cols + 1);
        columns.push(Arc::new(keys));

        for _ in 0..num_cols {
            let values: Float32Array = (start..end).map(|i| Some(i as f32)).collect();
            columns.push(Arc::new(values));
        }

        RecordBatch::try_new(Arc::new(schema.clone()), columns)
            .unwrap_or_else(|e| panic!("failed to create RecordBatch (rows {start}..{end}, {len} rows): {e}"))
    })
}
