from __future__ import annotations

import asyncio
import logging
import sys
import time

import pyperf

from murr_bench.backend import Backend
from murr_bench.testdata import column_names, generate_batches, generate_random_keys, make_schema

logger = logging.getLogger(__name__)


def run_benchmark(
    backend: Backend,
    bench_name: str,
    pyperf_args: list[str],
) -> None:
    config = backend.config

    logger.info("[%s] total_rows=%d, select_rows=%d, select_cols=%d, write_batch_size=%d",
                bench_name, config.total_rows, config.select_rows,
                config.select_cols, config.write_batch_size)

    logger.info("[%s] initializing backend...", bench_name)
    asyncio.run(backend.init())
    logger.info("[%s] backend ready", bench_name)

    columns = column_names(config.select_cols)
    schema = make_schema(config.select_cols)
    num_batches = -(-config.total_rows // config.write_batch_size)  # ceil div

    logger.info("[%s] writing %d rows in %d batches...",
                bench_name, config.total_rows, num_batches)

    async def _load_data() -> None:
        ingest_start = time.monotonic()
        last_log = time.monotonic()
        for i, batch in enumerate(
            generate_batches(schema, config.total_rows, config.write_batch_size)
        ):
            await backend.write_batch(batch)
            now = time.monotonic()
            if i + 1 == num_batches or now - last_log >= 5.0:
                logger.info("[%s] wrote batch %d/%d", bench_name, i + 1, num_batches)
                last_log = now

        ingest_elapsed = time.monotonic() - ingest_start
        logger.info("[%s] ingest total: %.2fs (%.0f rows/s)",
                    bench_name, ingest_elapsed,
                    config.total_rows / ingest_elapsed)

    asyncio.run(_load_data())

    logger.info("[%s] starting benchmark...", bench_name)

    async def bench_read() -> None:
        keys = generate_random_keys(config.select_rows, config.total_rows)
        await backend.read(keys, columns)

    pyperf_cli = [
        f"--warmup-time={config.warmup_time_secs}",
        f"--values={config.sample_size}",
        f"--min-time={config.measurement_time_secs}",
        "--processes=1",
    ] + pyperf_args
    sys.argv = [sys.argv[0]] + pyperf_cli

    runner = pyperf.Runner()
    runner.bench_async_func(
        f"{bench_name}/rows_{config.total_rows}/keys_{config.select_rows}",
        bench_read,
    )

    logger.info("[%s] cleaning up...", bench_name)
    asyncio.run(backend.cleanup())
    logger.info("[%s] done", bench_name)
