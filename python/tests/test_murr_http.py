from murr_bench.backends.murr_http import MurrHttp
from murr_bench.config import MurrHttpConfig
from tests.conftest import *


async def test_roundtrip():
    backend = MurrHttp(MurrHttpConfig(
        total_rows=TOTAL_ROWS,
        select_rows=SELECT_ROWS,
        select_cols=SELECT_COLS,
        write_batch_size=WRITE_BATCH_SIZE,
        measurement_time_secs=MEASUREMENT_TIME_SECS,
        warmup_time_secs=WARMUP_TIME_SECS,
        sample_size=SAMPLE_SIZE,
        backend=MurrHttpConfig.Backend(image="ghcr.io/murrdb/murr:latest"),
    ))
    await roundtrip(backend)
