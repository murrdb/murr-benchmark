import tempfile

from murr_bench.backends.rocksdb import RocksDb
from murr_bench.config import RocksDbConfig
from tests.conftest import *


async def test_roundtrip():
    with tempfile.TemporaryDirectory() as tmp:
        backend = RocksDb(RocksDbConfig(
            total_rows=TOTAL_ROWS,
            select_rows=SELECT_ROWS,
            select_cols=SELECT_COLS,
            write_batch_size=WRITE_BATCH_SIZE,
            measurement_time_secs=MEASUREMENT_TIME_SECS,
            warmup_time_secs=WARMUP_TIME_SECS,
            sample_size=SAMPLE_SIZE,
            backend=RocksDbConfig.Backend(data_dir=tmp),
        ))
        await roundtrip(backend)
