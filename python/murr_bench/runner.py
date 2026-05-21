from __future__ import annotations

import asyncio
import logging
import sys
import time

import pyperf

from murr_bench.backend import Backend
from murr_bench.testdata import column_names, generate_batches, generate_random_keys, make_schema

logger = logging.getLogger(__name__)


class PersistentEventLoop(asyncio.SelectorEventLoop):
    """Event loop that ignores close() — for reuse across pyperf iterations."""

    def close(self) -> None:
        pass

    def real_close(self) -> None:
        super().close()


def run_benchmark(
    backend: Backend,
    bench_name: str,
    pyperf_args: list[str],
) -> None:
    config = backend.config

    logger.info("[%s] total_rows=%d, select_rows=%d, select_cols=%d, write_batch_size=%d",
                bench_name, config.total_rows, config.select_rows,
                config.select_cols, config.write_batch_size)

    columns = column_names(config.select_cols)
    schema = make_schema(config.select_cols)
    num_batches = -(-config.total_rows // config.write_batch_size)  # ceil div

    loop = PersistentEventLoop()
    asyncio.set_event_loop(loop)

    logger.info("[%s] initializing backend...", bench_name)
    loop.run_until_complete(backend.init())
    logger.info("[%s] backend ready", bench_name)

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

    loop.run_until_complete(_load_data())

    logger.info("[%s] flushing backend...", bench_name)
    loop.run_until_complete(backend.flush())

    # Calibrate `loops` from a few real calls. pyperf's `--worker` mode (which we
    # need because the backend is initialised in this process) requires an explicit
    # `--loops=N` and does not auto-calibrate. Target ~100ms per value, matching
    # pyperf's own default value-time target.
    TARGET_VALUE_MS = 100.0
    LOOPS_CAP = 10000  # bound keys_pool memory for very-fast backends

    async def _calibrate_loops() -> int:
        # Discard the first call as warmup (connection setup, page-cache fill, etc.).
        await backend.read(
            generate_random_keys(config.select_rows, config.total_rows), columns
        )
        n_cal = 5
        t0 = pyperf.perf_counter()
        for _ in range(n_cal):
            keys = generate_random_keys(config.select_rows, config.total_rows)
            await backend.read(keys, columns)
        per_call_ms = (pyperf.perf_counter() - t0) * 1000.0 / n_cal
        loops = max(1, int(TARGET_VALUE_MS / per_call_ms))
        loops = min(loops, LOOPS_CAP)
        logger.info(
            "[%s] calibrated: %.3fms/call -> loops=%d", bench_name, per_call_ms, loops
        )
        return loops

    loops = loop.run_until_complete(_calibrate_loops())

    logger.info("[%s] starting benchmark...", bench_name)

    # Mirrors Criterion's `iter_batched` semantics: keys are pre-generated outside
    # the timed region so RNG + str() cost is excluded from the measurement.
    def bench_time_func(loops: int) -> float:
        keys_pool = [
            generate_random_keys(config.select_rows, config.total_rows)
            for _ in range(loops)
        ]
        t0 = pyperf.perf_counter()
        for keys in keys_pool:
            loop.run_until_complete(backend.read(keys, columns))
        return pyperf.perf_counter() - t0

    # `measurement_time_secs` is a soft hint on the pyperf side: each value is
    # ~TARGET_VALUE_MS, then pyperf runs `sample_size` values. Total runtime is
    # roughly sample_size * 100ms regardless of measurement_time_secs. Use
    # `sample_size` to control bench length; `warmup_time_secs` is interpreted as
    # a count of warmup values (not seconds — pyperf has no time-based equivalent).
    pyperf_cli = [
        f"--values={config.sample_size}",
        f"--warmups={config.warmup_time_secs}",
        "--worker",
        f"--loops={loops}",
    ] + pyperf_args
    sys.argv = [sys.argv[0]] + pyperf_cli

    runner = pyperf.Runner()
    runner.bench_time_func(
        f"{bench_name}/rows_{config.total_rows}/keys_{config.select_rows}",
        bench_time_func,
    )

    logger.info("[%s] cleaning up...", bench_name)
    loop.run_until_complete(backend.cleanup())
    loop.real_close()
    logger.info("[%s] done", bench_name)
