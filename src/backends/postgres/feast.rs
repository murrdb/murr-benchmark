use std::borrow::Cow;
use std::sync::Arc;

use serde::Deserialize;
use tokio_postgres::binary_copy::BinaryCopyInWriter;
use tokio_postgres::types::{ToSql, Type};

use crate::backend::Backend;
use crate::codec::KeyEncoder;
use crate::codec::key::StringKey;
use crate::config::{BackendConfig, DbConfig};
use crate::workload::{Request, RowBatch, Schema};

use super::{
    PgContainer, default_effective_cache_size, default_shared_buffers, default_work_mem,
};

#[derive(Debug, Clone, Deserialize)]
pub struct PgFeastConfig {
    pub image: String,
    #[serde(default)]
    pub cgroup_memory_mb: Option<i64>,
    #[serde(default = "default_shared_buffers")]
    pub shared_buffers: String,
    #[serde(default = "default_work_mem")]
    pub work_mem: String,
    #[serde(default = "default_effective_cache_size")]
    pub effective_cache_size: String,
}

impl BackendConfig for PgFeastConfig {}

const KEY_COLUMN: &str = "key";

/// Column names can be arbitrary (numeric feature IDs, for instance), so they are always quoted.
fn quote_ident(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

#[derive(Clone)]
pub struct PgFeast {
    pg: PgContainer,
    read_stmt: Arc<tokio_postgres::Statement>,
    copy_sql: Arc<str>,
    copy_types: Arc<[Type]>,
    key: StringKey,
}

impl Backend for PgFeast {
    type Config = PgFeastConfig;
    type Response = Vec<tokio_postgres::Row>;

    async fn init(config: &DbConfig<Self::Config>, schema: Arc<Schema>) -> Self {
        let pg = PgContainer::start(
            &config.backend.image,
            config.backend.cgroup_memory_mb,
            &config.backend.shared_buffers,
            &config.backend.work_mem,
            &config.backend.effective_cache_size,
        )
        .await;

        // The key is stored as one string column, whatever the number of key fields.
        let key = StringKey::new(&schema);

        let col_defs: Vec<String> = schema
            .values
            .iter()
            .map(|f| {
                let null = if f.nullable { "" } else { " NOT NULL" };
                format!("{} {}{null}", quote_ident(&f.name), Type::from(f.dtype).name())
            })
            .collect();
        let ddl = format!(
            "CREATE TABLE bench ({KEY_COLUMN} TEXT PRIMARY KEY, {})",
            col_defs.join(", ")
        );
        pg.client
            .execute(&ddl, &[])
            .await
            .expect("failed to create table");

        let col_names: Vec<String> = schema.values.iter().map(|f| quote_ident(&f.name)).collect();
        let col_list = col_names.join(", ");
        let read_sql = format!("SELECT {col_list} FROM bench WHERE {KEY_COLUMN} = ANY($1)");
        let read_stmt = pg
            .client
            .prepare_typed(&read_sql, &[Type::TEXT_ARRAY])
            .await
            .expect("failed to prepare read statement");

        let copy_sql = format!("COPY bench ({KEY_COLUMN}, {col_list}) FROM STDIN BINARY");
        let copy_types: Vec<Type> = std::iter::once(Type::TEXT)
            .chain(schema.values.iter().map(|f| f.dtype.into()))
            .collect();

        PgFeast {
            pg,
            read_stmt: Arc::new(read_stmt),
            copy_sql: copy_sql.into(),
            copy_types: copy_types.into(),
            key,
        }
    }

    async fn write_batch(&self, batch: &RowBatch) {
        let sink = self.pg.client.copy_in(&*self.copy_sql).await.unwrap();
        let writer = BinaryCopyInWriter::new(sink, &self.copy_types);
        tokio::pin!(writer);

        for row in &batch.rows {
            let key = self.key.encode(&row.key);
            let mut row_refs: Vec<&(dyn ToSql + Sync)> = Vec::with_capacity(self.copy_types.len());
            row_refs.push(&key);
            for value in &row.values {
                row_refs.push(value);
            }
            writer.as_mut().write(&row_refs).await.unwrap();
        }
        writer.finish().await.unwrap();
    }

    async fn flush(&self) {
        self.pg
            .client
            .execute("ANALYZE bench", &[])
            .await
            .expect("analyze failed");
        self.pg
            .client
            .execute("CHECKPOINT", &[])
            .await
            .expect("checkpoint failed");
    }

    async fn read(&self, request: &Request) -> Self::Response {
        let keys: Vec<Cow<str>> = request.keys.iter().map(|k| self.key.encode(k)).collect();
        self.pg
            .client
            .query(&*self.read_stmt, &[&keys])
            .await
            .unwrap()
    }

    async fn memory_usage(&self) -> crate::backend::MemoryUsage {
        crate::stats::mem::MemoryUsage::for_container(self.pg._container.id()).await
    }

    async fn disk_usage(&self) -> crate::backend::DiskUsage {
        self.pg.disk_usage().await
    }

    async fn network_usage(&self) -> crate::backend::NetworkUsage {
        crate::stats::net::NetworkUsage::for_container(self.pg._container.id()).await
    }

    async fn cleanup(self) {
        drop(self.pg);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::DbConfig;
    use crate::testing::test_backend_roundtrip;

    #[tokio::test]
    async fn roundtrip() {
        let config = DbConfig {
            write_batch_size: 50,
            measurement_time_secs: 1,
            warmup_time_secs: 1,
            sample_size: 1,
            backend: PgFeastConfig {
                image: "postgres:18.6".to_string(),
                cgroup_memory_mb: None,
                shared_buffers: default_shared_buffers(),
                work_mem: default_work_mem(),
                effective_cache_size: default_effective_cache_size(),
            },
        };
        test_backend_roundtrip::<PgFeast>(config).await;
    }
}
