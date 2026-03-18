from __future__ import annotations

from murr_bench.backend import Backend
from murr_bench.testdata import column_names, generate_batches, make_schema

TOTAL_ROWS = 100
SELECT_ROWS = 10
SELECT_COLS = 2
WRITE_BATCH_SIZE = 50
MEASUREMENT_TIME_SECS = 1
WARMUP_TIME_SECS = 1
SAMPLE_SIZE = 1


async def roundtrip(backend: Backend) -> None:
    """Shared integration test: init -> write -> read -> cleanup."""
    config = backend.config
    await backend.init()

    schema = make_schema(config.select_cols)
    for batch in generate_batches(schema, config.total_rows, config.write_batch_size):
        await backend.write_batch(batch)

    await backend.flush()

    keys = [str(i) for i in range(config.select_rows)]
    columns = column_names(config.select_cols)
    df = await backend.read(keys, columns)

    assert len(df) == config.select_rows
    assert list(df.columns) == columns

    await backend.cleanup()
