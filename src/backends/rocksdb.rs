use std::borrow::Cow;
use std::path::PathBuf;
use std::sync::Arc;

use rayon::prelude::*;
use serde::Deserialize;

use crate::backend::Backend;
use crate::codec::blob::BlobRow;
use crate::codec::key::StringKey;
use crate::codec::{KeyEncoder, RowEncoder};
use crate::config::{BackendConfig, DbConfig};
use crate::workload::{Request, RowBatch, Schema};

#[derive(Debug, Clone, Copy, Deserialize, Default, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum TableFormat {
    #[default]
    BlockBased,
    Plain,
}

/// Read strategy, mirroring murr's `ReadMethod`.
#[derive(Debug, Clone, Copy, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ReadMethod {
    /// One `batched_multi_get` for all keys; RocksDB sorts internally.
    MultiGet,
    /// Pre-sort keys client-side, one `batched_multi_get` with `sorted_input = true`.
    MultiGetSorted,
    /// Sequential point `get` per key.
    Get,
    /// Point `get` per key, fanned out over rayon (murr's PlainTable default).
    ParGet,
    /// Keys chunked per rayon thread, one `batched_multi_get` per chunk (murr's block default).
    ParMultiGet,
}

#[derive(Debug, Clone, Deserialize)]
pub struct RocksDbConfig {
    pub data_dir: PathBuf,
    #[serde(default)]
    pub table_format: TableFormat,
    /// Defaults to murr's per-format default: `par_get` for plain, `par_multi_get` for block.
    #[serde(default)]
    pub read_method: Option<ReadMethod>,
    /// PlainTable: bloom filter bits per key (built into the table). 0 disables.
    #[serde(default = "default_plain_bloom_bits")]
    pub plain_bloom_bits_per_key: i32,
    /// PlainTable: hash table utilization ratio (lower = fewer collisions, more space).
    #[serde(default = "default_data_block_hash_ratio")]
    pub plain_hash_table_ratio: f64,
    /// PlainTable: stride between sparse-index entries (lower = faster lookup, more memory).
    #[serde(default = "default_plain_index_sparseness")]
    pub plain_index_sparseness: usize,
    /// PlainTable: store the full index in the file instead of rebuilding at open.
    #[serde(default)]
    pub plain_store_index_in_file: bool,
    #[serde(default)]
    pub bloom_filter_bits_per_key: Option<f64>,
    #[serde(default = "default_true")]
    pub whole_key_filtering: bool,
    #[serde(default = "default_block_size")]
    pub block_size: usize,
    #[serde(default)]
    pub block_cache_mb: usize,
    #[serde(default)]
    pub cache_index_and_filter_blocks: bool,
    #[serde(default)]
    pub pin_l0_filter_and_index_blocks: bool,
    #[serde(default = "default_block_restart_interval")]
    pub block_restart_interval: i32,
    #[serde(default = "default_true")]
    pub data_block_hash_index: bool,
    #[serde(default = "default_data_block_hash_ratio")]
    pub data_block_hash_ratio: f64,
    #[serde(default = "default_true")]
    pub mmap_reads: bool,
    /// Open SST files with O_DIRECT, bypassing the OS page cache. Mutually exclusive with `mmap_reads`.
    #[serde(default)]
    pub use_direct_reads: bool,
    #[serde(default = "default_true")]
    pub async_io: bool,
    #[serde(default)]
    pub verify_checksums: bool,
    /// Memtable size before flush. murr default: 256 MiB.
    #[serde(default = "default_write_buffer_size")]
    pub write_buffer_size: usize,
    /// Target SST file size. murr default: 1 GiB.
    #[serde(default = "default_target_file_size_base")]
    pub target_file_size_base: u64,
    #[serde(default)]
    pub disable_auto_compactions: bool,
}

impl RocksDbConfig {
    fn effective_read_method(&self) -> ReadMethod {
        self.read_method.unwrap_or(match self.table_format {
            TableFormat::Plain => ReadMethod::ParGet,
            TableFormat::BlockBased => ReadMethod::ParMultiGet,
        })
    }
}

fn default_true() -> bool {
    true
}
fn default_write_buffer_size() -> usize {
    256 * 1024 * 1024
}
fn default_target_file_size_base() -> u64 {
    1024 * 1024 * 1024
}
fn default_block_size() -> usize {
    512
}
fn default_block_restart_interval() -> i32 {
    8
}
fn default_data_block_hash_ratio() -> f64 {
    0.75
}
fn default_plain_bloom_bits() -> i32 {
    16
}
fn default_plain_index_sparseness() -> usize {
    4
}

impl BackendConfig for RocksDbConfig {}

#[derive(Clone)]
pub struct RocksDb {
    db: Arc<rocksdb::DB>,
    data_dir: PathBuf,
    async_io: bool,
    verify_checksums: bool,
    read_method: ReadMethod,
    key: StringKey,
    blob: BlobRow,
}

impl Backend for RocksDb {
    type Config = RocksDbConfig;
    type Response = Vec<Option<Vec<u8>>>;

    async fn init(config: &DbConfig<Self::Config>, schema: Arc<Schema>) -> Self {
        let cfg = &config.backend;
        let data_dir = &cfg.data_dir;
        match std::fs::read_dir(data_dir) {
            Ok(mut entries) => {
                if entries.next().is_some() {
                    panic!(
                        "RocksDB data_dir {} is not empty; remove it before running the benchmark",
                        data_dir.display()
                    );
                }
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                std::fs::create_dir_all(data_dir).expect("failed to create data_dir");
            }
            Err(e) => panic!("failed to inspect data_dir {}: {e}", data_dir.display()),
        }

        let mut opts = rocksdb::Options::default();
        opts.create_if_missing(true);
        opts.set_write_buffer_size(cfg.write_buffer_size);
        opts.set_target_file_size_base(cfg.target_file_size_base);
        opts.set_disable_auto_compactions(cfg.disable_auto_compactions);

        match cfg.table_format {
            TableFormat::BlockBased => {
                let mut block_opts = rocksdb::BlockBasedOptions::default();
                block_opts.set_block_size(cfg.block_size);
                if let Some(bits) = cfg.bloom_filter_bits_per_key {
                    block_opts.set_bloom_filter(bits, false);
                }
                block_opts.set_whole_key_filtering(cfg.whole_key_filtering);
                block_opts.set_cache_index_and_filter_blocks(cfg.cache_index_and_filter_blocks);
                block_opts.set_pin_l0_filter_and_index_blocks_in_cache(
                    cfg.pin_l0_filter_and_index_blocks,
                );
                // Like murr: 0 leaves RocksDB's default 32 MiB internal block cache in place
                // rather than setting no_block_cache, which makes every read re-parse blocks.
                if cfg.block_cache_mb > 0 {
                    let cache = rocksdb::Cache::new_lru_cache(cfg.block_cache_mb * 1024 * 1024);
                    block_opts.set_block_cache(&cache);
                }
                block_opts.set_block_restart_interval(cfg.block_restart_interval);
                if cfg.data_block_hash_index {
                    block_opts
                        .set_data_block_index_type(rocksdb::DataBlockIndexType::BinaryAndHash);
                    block_opts.set_data_block_hash_ratio(cfg.data_block_hash_ratio);
                }
                opts.set_allow_mmap_reads(cfg.mmap_reads);
                opts.set_use_direct_reads(cfg.use_direct_reads);
                opts.set_block_based_table_factory(&block_opts);
            }
            TableFormat::Plain => {
                // PlainTable requires mmap and is incompatible with O_DIRECT.
                assert!(
                    !cfg.use_direct_reads,
                    "table_format=plain is incompatible with use_direct_reads"
                );
                opts.set_allow_mmap_reads(true);
                // PlainTable requires a prefix extractor; noop = whole key as prefix.
                opts.set_prefix_extractor(rocksdb::SliceTransform::create_noop());
                // murr uses a Vector memtable for PlainTable (append-only, sorted at flush).
                opts.set_memtable_factory(rocksdb::MemtableFactory::Vector);
                let plain_opts = rocksdb::PlainTableFactoryOptions {
                    user_key_length: 0,
                    bloom_bits_per_key: cfg.plain_bloom_bits_per_key,
                    hash_table_ratio: cfg.plain_hash_table_ratio,
                    index_sparseness: cfg.plain_index_sparseness,
                    huge_page_tlb_size: 0,
                    encoding_type: rocksdb::KeyEncodingType::Plain,
                    full_scan_mode: false,
                    store_index_in_file: cfg.plain_store_index_in_file,
                };
                opts.set_plain_table_factory(&plain_opts);
            }
        }

        // open_cf_with_opts so the default CF is registered in the handle map
        // (DB::open leaves it unregistered, breaking cf_handle("default")).
        let db = rocksdb::DB::open_cf_with_opts(
            &opts,
            data_dir,
            [(rocksdb::DEFAULT_COLUMN_FAMILY_NAME, opts.clone())],
        )
        .expect("failed to open RocksDB");

        RocksDb {
            db: Arc::new(db),
            data_dir: data_dir.clone(),
            async_io: cfg.async_io,
            verify_checksums: cfg.verify_checksums,
            read_method: cfg.effective_read_method(),
            key: StringKey::new(&schema),
            blob: BlobRow::new(&schema),
        }
    }

    async fn write_batch(&self, batch: &RowBatch) {
        let mut wb = rocksdb::WriteBatch::default();

        let mut blob = Vec::new();
        for row in &batch.rows {
            blob.clear();
            self.blob.encode(row, &mut blob);
            wb.put(self.key.encode(&row.key).as_bytes(), &blob);
        }

        self.db.write(wb).expect("failed to write batch");
        // murr flushes the memtable after every write call; mirror it so ingest cost and
        // L0 layout match.
        self.db.flush().expect("failed to flush RocksDB");
    }

    async fn flush(&self) {
        self.db.flush().expect("failed to flush RocksDB");
        self.db.compact_range::<&[u8], &[u8]>(None, None);
    }

    async fn read(&self, request: &Request) -> Self::Response {
        let encoded: Vec<Cow<str>> = request.keys.iter().map(|k| self.key.encode(k)).collect();
        let keys: Vec<&[u8]> = encoded.iter().map(|k| k.as_bytes()).collect();
        let n = keys.len();
        let cf = self.db.cf_handle("default").expect("default CF missing");

        let mut ropts = rocksdb::ReadOptions::default();
        ropts.set_async_io(self.async_io);
        ropts.set_verify_checksums(self.verify_checksums);

        let to_owned = |r: Result<Option<rocksdb::DBPinnableSlice<'_>>, rocksdb::Error>| {
            r.expect("rocksdb read error").map(|s| s.to_vec())
        };

        match self.read_method {
            ReadMethod::MultiGet => self
                .db
                .batched_multi_get_cf_opt(&cf, &keys, false, &ropts)
                .into_iter()
                .map(to_owned)
                .collect(),
            ReadMethod::MultiGetSorted => {
                // Pre-sort keys so RocksDB can skip its internal sort, then permute results back.
                let mut order: Vec<usize> = (0..n).collect();
                order.sort_unstable_by_key(|&i| keys[i]);
                let sorted_keys: Vec<&[u8]> = order.iter().map(|&i| keys[i]).collect();

                let results = self.db.batched_multi_get_cf_opt(&cf, &sorted_keys, true, &ropts);

                let mut output: Vec<Option<Vec<u8>>> = (0..n).map(|_| None).collect();
                for (sorted_pos, result) in results.into_iter().enumerate() {
                    output[order[sorted_pos]] = to_owned(result);
                }
                output
            }
            ReadMethod::Get => keys
                .iter()
                .map(|k| to_owned(self.db.get_pinned_cf_opt(&cf, k, &ropts)))
                .collect(),
            ReadMethod::ParGet => keys
                .par_iter()
                .map(|k| to_owned(self.db.get_pinned_cf_opt(&cf, k, &ropts)))
                .collect(),
            ReadMethod::ParMultiGet => {
                let chunk_size = n.div_ceil(rayon::current_num_threads()).max(1);
                keys.par_chunks(chunk_size)
                    .flat_map_iter(|chunk| {
                        self.db
                            .batched_multi_get_cf_opt(&cf, chunk, false, &ropts)
                            .into_iter()
                            .map(to_owned)
                    })
                    .collect()
            }
        }
    }

    async fn memory_usage(&self) -> crate::backend::MemoryUsage {
        let block_cache = self
            .db
            .property_int_value("rocksdb.block-cache-usage")
            .ok()
            .flatten()
            .unwrap_or(0);
        let table_readers = self
            .db
            .property_int_value("rocksdb.estimate-table-readers-mem")
            .ok()
            .flatten()
            .unwrap_or(0);
        let memtable = self
            .db
            .property_int_value("rocksdb.cur-size-all-mem-tables")
            .ok()
            .flatten()
            .unwrap_or(0);
        let engine = block_cache + table_readers + memtable;
        let shared_bytes = crate::stats::mem::MemoryUsage::for_process().shared_bytes;
        crate::backend::MemoryUsage {
            rss_bytes: engine,
            shared_bytes,
            total_bytes: shared_bytes + engine,
        }
    }

    async fn disk_usage(&self) -> crate::backend::DiskUsage {
        crate::stats::disk::DiskUsage::for_path(&self.data_dir)
    }

    async fn cleanup(self) {
        drop(self.db);
        let _ = std::fs::remove_dir_all(&self.data_dir);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::DbConfig;
    use crate::testing::test_backend_roundtrip;
    use tempfile::TempDir;

    fn base_config(dir: &TempDir) -> DbConfig<RocksDbConfig> {
        DbConfig {
            write_batch_size: 50,
            measurement_time_secs: 1,
            warmup_time_secs: 1,
            sample_size: 1,
            backend: RocksDbConfig {
                data_dir: dir.path().to_path_buf(),
                table_format: TableFormat::BlockBased,
                read_method: None,
                plain_bloom_bits_per_key: default_plain_bloom_bits(),
                plain_hash_table_ratio: default_data_block_hash_ratio(),
                plain_index_sparseness: default_plain_index_sparseness(),
                plain_store_index_in_file: false,
                bloom_filter_bits_per_key: None,
                whole_key_filtering: true,
                block_size: default_block_size(),
                block_cache_mb: 0,
                cache_index_and_filter_blocks: false,
                pin_l0_filter_and_index_blocks: false,
                block_restart_interval: default_block_restart_interval(),
                data_block_hash_index: true,
                data_block_hash_ratio: default_data_block_hash_ratio(),
                mmap_reads: true,
                use_direct_reads: false,
                async_io: true,
                verify_checksums: false,
                write_buffer_size: default_write_buffer_size(),
                target_file_size_base: default_target_file_size_base(),
                disable_auto_compactions: false,
            },
        }
    }

    #[tokio::test]
    async fn roundtrip_block_based() {
        let dir = TempDir::new().unwrap();
        let config = base_config(&dir);
        test_backend_roundtrip::<RocksDb>(config).await;
    }

    #[tokio::test]
    async fn roundtrip_plain_table() {
        let dir = TempDir::new().unwrap();
        let mut config = base_config(&dir);
        config.backend.table_format = TableFormat::Plain;
        test_backend_roundtrip::<RocksDb>(config).await;
    }

    #[tokio::test]
    async fn roundtrip_all_read_methods() {
        const METHODS: [ReadMethod; 5] = [
            ReadMethod::MultiGet,
            ReadMethod::MultiGetSorted,
            ReadMethod::Get,
            ReadMethod::ParGet,
            ReadMethod::ParMultiGet,
        ];
        for format in [TableFormat::BlockBased, TableFormat::Plain] {
            for method in METHODS {
                let dir = TempDir::new().unwrap();
                let mut config = base_config(&dir);
                config.backend.table_format = format;
                config.backend.read_method = Some(method);
                test_backend_roundtrip::<RocksDb>(config).await;
            }
        }
    }
}
