use crate::backend::Backend;
use crate::config::DbConfig;
use crate::workload::synthetic::{SyntheticConfig, SyntheticWorkload};
use crate::workload::{Key, Request, Row, RowBatch, Value, Workload};

/// Shared integration test: init → write → flush → read → cleanup.
///
/// Verifies the full backend lifecycle completes without panicking.
/// Uses a small synthetic workload for fast execution.
pub async fn test_backend_roundtrip<B: Backend>(config: DbConfig<B::Config>) {
    let workload = SyntheticWorkload::new(SyntheticConfig {
        total_rows: 100,
        select_rows: 10,
        select_cols: 2,
    });
    let schema = workload.schema();
    let backend = B::init(&config, schema.clone()).await;

    let rows: Vec<Row> = workload.rows().collect();
    for chunk in rows.chunks(config.write_batch_size) {
        let batch = RowBatch {
            rows: chunk.to_vec(),
        };
        backend.write_batch(&batch).await;
    }
    backend.flush().await;

    let mem = backend.memory_usage().await;
    assert!(mem.rss_bytes > 0, "expected non-zero RSS");
    assert!(mem.total_bytes > 0, "expected non-zero TOTAL");

    // Read back known keys (first select_rows keys: "0", "1", ...)
    let request = Request {
        keys: (0..workload.keys_per_request())
            .map(|i| Key(vec![Value::Utf8(i.to_string())]))
            .collect(),
        columns: schema.values.iter().map(|f| f.name.clone()).collect(),
    };
    let _response = backend.read(&request).await;

    backend.cleanup().await;
}
