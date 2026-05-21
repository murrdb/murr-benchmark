from __future__ import annotations

import argparse
import logging
import sys

import pyperf

from murr_bench.backends import DEFAULT_CONFIGS, get_backends
from murr_bench.runner import run_benchmark


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    logging.getLogger("httpx").setLevel(logging.WARNING)

    parser = argparse.ArgumentParser(
        description="murr benchmark (Python e2e)",
        add_help=False,
    )
    parser.add_argument("backend", choices=sorted(DEFAULT_CONFIGS.keys()))
    parser.add_argument("--config", help="YAML config path (default: configs/<backend>.yaml)")

    args, remaining = parser.parse_known_args()

    variants = get_backends(args.backend, args.config)
    if not variants:
        raise SystemExit(f"no variants found in config for backend {args.backend!r}")

    # One Runner per process (pyperf enforces this). --loops=1 here is a sentinel:
    # each variant passes its calibrated loop count as inner_loops on bench_time_func,
    # so pyperf still computes per-iteration time correctly across variants.
    shared = variants[0][1].config
    pyperf_cli = [
        f"--values={shared.sample_size}",
        f"--warmups={shared.warmup_time_secs}",
        "--worker",
        "--loops=1",
    ] + remaining
    sys.argv = [sys.argv[0]] + pyperf_cli
    runner = pyperf.Runner()

    for variant_name, backend in variants:
        run_benchmark(runner, backend, f"{args.backend}/{variant_name}")


if __name__ == "__main__":
    main()
