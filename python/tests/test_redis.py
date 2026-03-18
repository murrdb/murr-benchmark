from murr_bench.backends.redis import RedisFeast, RedisFeatureBlob
from murr_bench.config import RedisFeastConfig, RedisFeatureBlobConfig
from tests.conftest import *


async def test_feast_hgetall():
    backend = RedisFeast(RedisFeastConfig(
        total_rows=TOTAL_ROWS,
        select_rows=SELECT_ROWS,
        select_cols=SELECT_COLS,
        write_batch_size=WRITE_BATCH_SIZE,
        measurement_time_secs=MEASUREMENT_TIME_SECS,
        warmup_time_secs=WARMUP_TIME_SECS,
        sample_size=SAMPLE_SIZE,
        backend=RedisFeastConfig.Backend(image="redis:latest", read_mode="hgetall"),
    ))
    await roundtrip(backend)


async def test_feast_hmget():
    backend = RedisFeast(RedisFeastConfig(
        total_rows=TOTAL_ROWS,
        select_rows=SELECT_ROWS,
        select_cols=SELECT_COLS,
        write_batch_size=WRITE_BATCH_SIZE,
        measurement_time_secs=MEASUREMENT_TIME_SECS,
        warmup_time_secs=WARMUP_TIME_SECS,
        sample_size=SAMPLE_SIZE,
        backend=RedisFeastConfig.Backend(image="redis:latest", read_mode="hmget"),
    ))
    await roundtrip(backend)


async def test_featureblob():
    backend = RedisFeatureBlob(RedisFeatureBlobConfig(
        total_rows=TOTAL_ROWS,
        select_rows=SELECT_ROWS,
        select_cols=SELECT_COLS,
        write_batch_size=WRITE_BATCH_SIZE,
        measurement_time_secs=MEASUREMENT_TIME_SECS,
        warmup_time_secs=WARMUP_TIME_SECS,
        sample_size=SAMPLE_SIZE,
        backend=RedisFeatureBlobConfig.Backend(image="redis:latest"),
    ))
    await roundtrip(backend)
