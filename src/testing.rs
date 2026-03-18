use arrow::array::AsArray;

use crate::backend::{Backend, Batch};
use crate::config::BenchConfig;
use crate::testdata;

/// Shared integration test: init → write → read → cleanup.
///
/// Verifies the full backend lifecycle completes without panicking.
/// Uses small parameters for fast execution.
pub async fn test_backend_roundtrip<B: Backend>(config: BenchConfig<B::Config>) {
    let backend = B::init(&config).await;

    let columns = testdata::column_names(config.select_cols);
    let schema = testdata::make_schema(config.select_cols);
    for record_batch in
        testdata::generate_batches(&schema, config.total_rows, config.write_batch_size)
    {
        let keys: Vec<String> = record_batch
            .column(0)
            .as_string::<i32>()
            .iter()
            .map(|v| v.unwrap().to_string())
            .collect();
        let batch = Batch {
            inner: record_batch,
            keys,
            columns: columns.clone(),
        };
        backend.write_batch(&batch).await;
    }

    // Read back known keys (first select_rows keys: "0", "1", ...)
    let keys: Vec<String> = (0..config.select_rows).map(|i| i.to_string()).collect();
    let _response = backend.read(&keys, &columns).await;

    backend.cleanup().await;
}
