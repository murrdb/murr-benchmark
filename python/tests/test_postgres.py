from murr_bench.backends.postgres import PgFeast, PgFeatureBlob
from murr_bench.config import PgFeastConfig, PgFeatureBlobConfig
from tests.conftest import *


async def test_feast():
    backend = PgFeast(PgFeastConfig(
        total_rows=TOTAL_ROWS,
        select_rows=SELECT_ROWS,
        select_cols=SELECT_COLS,
        write_batch_size=WRITE_BATCH_SIZE,
        measurement_time_secs=MEASUREMENT_TIME_SECS,
        warmup_time_secs=WARMUP_TIME_SECS,
        sample_size=SAMPLE_SIZE,
        backend=PgFeastConfig.Backend(image="postgres:17"),
    ))
    await roundtrip(backend)


async def test_featureblob():
    backend = PgFeatureBlob(PgFeatureBlobConfig(
        total_rows=TOTAL_ROWS,
        select_rows=SELECT_ROWS,
        select_cols=SELECT_COLS,
        write_batch_size=WRITE_BATCH_SIZE,
        measurement_time_secs=MEASUREMENT_TIME_SECS,
        warmup_time_secs=WARMUP_TIME_SECS,
        sample_size=SAMPLE_SIZE,
        backend=PgFeatureBlobConfig.Backend(image="postgres:17"),
    ))
    await roundtrip(backend)
