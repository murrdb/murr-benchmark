from __future__ import annotations

import argparse
import logging

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
    for variant_name, backend in variants:
        run_benchmark(backend, f"{args.backend}/{variant_name}", remaining)


if __name__ == "__main__":
    main()
