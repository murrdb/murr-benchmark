from __future__ import annotations

import logging
import shutil

import numpy as np
import pandas as pd
import pyarrow as pa
from rocksdict import (
    BlockBasedOptions,
    Cache,
    DataBlockIndexType,
    KeyEncodingType,
    Options,
    PlainTableFactoryOptions,
    Rdict,
    ReadOptions,
    SliceTransform,
)

from murr_bench.backend import Backend
from murr_bench.config import RocksDbConfig

logger = logging.getLogger(__name__)


class RocksDb(Backend):
    def __init__(self, config: RocksDbConfig) -> None:
        self.config = config
        self._db: Rdict | None = None

    def _build_options(self) -> Options:
        cfg = self.config.backend
        opts = Options()
        opts.create_if_missing(True)

        if cfg.table_format == "block_based":
            block_opts = BlockBasedOptions()
            block_opts.set_block_size(cfg.block_size)
            if cfg.bloom_filter_bits_per_key is not None:
                block_opts.set_bloom_filter(cfg.bloom_filter_bits_per_key, False)
            if not cfg.whole_key_filtering:
                # rocksdict's BlockBasedOptions does not expose set_whole_key_filtering;
                # the underlying RocksDB default (True) will be used regardless.
                logger.warning(
                    "rocksdb: whole_key_filtering=False is not exposed by rocksdict; "
                    "ignoring (RocksDB default True applies)."
                )
            block_opts.set_cache_index_and_filter_blocks(cfg.cache_index_and_filter_blocks)
            block_opts.set_pin_l0_filter_and_index_blocks_in_cache(
                cfg.pin_l0_filter_and_index_blocks
            )
            if cfg.block_cache_mb > 0:
                block_opts.set_block_cache(Cache(cfg.block_cache_mb * 1024 * 1024))
            else:
                block_opts.disable_cache()
            block_opts.set_block_restart_interval(cfg.block_restart_interval)
            if cfg.data_block_hash_index:
                block_opts.set_data_block_index_type(
                    DataBlockIndexType.binary_and_hash()
                )
                block_opts.set_data_block_hash_ratio(cfg.data_block_hash_ratio)
            opts.set_allow_mmap_reads(cfg.mmap_reads)
            opts.set_use_direct_reads(cfg.use_direct_reads)
            opts.set_block_based_table_factory(block_opts)
        else:
            assert not cfg.use_direct_reads, (
                "table_format=plain is incompatible with use_direct_reads"
            )
            # PlainTable requires mmap and a prefix extractor (noop = whole key).
            opts.set_allow_mmap_reads(True)
            opts.set_prefix_extractor(SliceTransform.create_noop())
            plain_opts = PlainTableFactoryOptions()
            plain_opts.user_key_length = 0
            plain_opts.bloom_bits_per_key = cfg.plain_bloom_bits_per_key
            plain_opts.hash_table_ratio = cfg.plain_hash_table_ratio
            plain_opts.index_sparseness = cfg.plain_index_sparseness
            plain_opts.huge_page_tlb_size = 0
            plain_opts.encoding_type = KeyEncodingType.plain()
            plain_opts.full_scan_mode = False
            plain_opts.store_index_in_file = cfg.plain_store_index_in_file
            opts.set_plain_table_factory(plain_opts)

        return opts

    async def init(self) -> None:
        cfg = self.config.backend
        data_dir = cfg.data_dir
        if data_dir.exists():
            if any(data_dir.iterdir()):
                raise RuntimeError(
                    f"RocksDB data_dir {data_dir} is not empty; "
                    f"remove it before running the benchmark"
                )
        else:
            data_dir.mkdir(parents=True)

        self._db = Rdict(str(data_dir), options=self._build_options())

        ropts = ReadOptions()
        ropts.set_async_io(cfg.async_io)
        ropts.set_verify_checksums(cfg.verify_checksums)
        self._db.set_read_options(ropts)

        logger.info(
            "rocksdb: opened at %s (table_format=%s)",
            data_dir,
            cfg.table_format,
        )

    async def write_batch(self, batch: pa.RecordBatch) -> None:
        assert self._db is not None
        keys = batch.column("key").to_pylist()
        num_cols = batch.num_columns - 1
        values = np.column_stack(
            [batch.column(i + 1).to_numpy() for i in range(num_cols)]
        ).astype(np.float32)

        for row_idx, key in enumerate(keys):
            self._db[key] = values[row_idx].tobytes()

    async def read(self, keys: list[str], columns: list[str]) -> pd.DataFrame:
        assert self._db is not None
        blobs = self._db.get(keys)
        rows = np.stack(
            [np.frombuffer(b, dtype="<f4") for b in blobs if b is not None]
        )
        return pd.DataFrame(rows, columns=columns)

    async def flush(self) -> None:
        assert self._db is not None
        self._db.flush()
        self._db.compact_range(None, None)

    async def cleanup(self) -> None:
        if self._db is not None:
            self._db.close()
        shutil.rmtree(str(self.config.backend.data_dir), ignore_errors=True)
