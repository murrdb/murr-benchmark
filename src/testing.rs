use std::sync::Arc;

use crate::backend::Backend;
use crate::config::DbConfig;
use crate::workload::synthetic::{SyntheticConfig, SyntheticWorkload};
use crate::workload::{DType, Field, Key, Request, Row, RowBatch, Schema, Value, Workload};

/// Shared integration test: init → write → flush → read → cleanup.
///
/// Verifies the full backend lifecycle completes without panicking, once on a
/// small synthetic workload and once on data shaped like a recorded table.
pub async fn test_backend_roundtrip<B: Backend>(config: DbConfig<B::Config>) {
    roundtrip_synthetic::<B>(config.clone()).await;
    roundtrip_compound::<B>(config).await;
}

async fn roundtrip_synthetic<B: Backend>(config: DbConfig<B::Config>) {
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
        keys: (0..10)
            .map(|i| Key(vec![Value::Utf8(i.to_string())]))
            .collect(),
        columns: schema.values.iter().map(|f| f.name.clone()).collect(),
    };
    let _response = backend.read(&request).await;

    backend.cleanup().await;
}

/// A two-field key, numeric column names, mixed value types and nulls. The
/// request reads a subset of the columns and includes a missing key.
async fn roundtrip_compound<B: Backend>(config: DbConfig<B::Config>) {
    let field = |name: &str, dtype, nullable| Field {
        name: name.to_string(),
        dtype,
        nullable,
    };
    let schema = Arc::new(Schema {
        keys: vec![field("6", DType::Utf8, false), field("8", DType::Utf8, false)],
        values: vec![
            field("1226", DType::Utf8, true),
            field("1228", DType::Float64, true),
            field("1230", DType::Int64, true),
        ],
    });
    let key = |vendor: usize| {
        Key(vec![
            Value::Utf8("geo".to_string()),
            Value::Utf8(format!("vendor-{vendor}")),
        ])
    };
    let rows: Vec<Row> = (0..100)
        .map(|i| Row {
            key: key(i),
            values: vec![
                (i % 2 == 0).then(|| Value::Utf8(format!("name-{i}"))),
                (i % 3 != 0).then_some(Value::Float64(i as f64)),
                (i % 5 != 0).then_some(Value::Int64(i as i64)),
            ],
        })
        .collect();

    let backend = B::init(&config, schema).await;
    for chunk in rows.chunks(config.write_batch_size) {
        let batch = RowBatch {
            rows: chunk.to_vec(),
        };
        backend.write_batch(&batch).await;
    }
    backend.flush().await;

    let request = Request {
        keys: vec![key(0), key(1), key(15), key(1000)],
        columns: vec!["1228".to_string(), "1230".to_string()],
    };
    let _response = backend.read(&request).await;

    backend.cleanup().await;
}
