from __future__ import annotations

from collections.abc import Iterator

import numpy as np
import pyarrow as pa


def column_names(num_cols: int) -> list[str]:
    return [f"col_{i}" for i in range(num_cols)]


def make_schema(num_cols: int) -> pa.Schema:
    fields = [pa.field("key", pa.utf8(), nullable=False)]
    for name in column_names(num_cols):
        fields.append(pa.field(name, pa.float32(), nullable=False))
    return pa.schema(fields)


def generate_batches(
    schema: pa.Schema, total_rows: int, batch_size: int
) -> Iterator[pa.RecordBatch]:
    num_cols = len(schema) - 1
    for start in range(0, total_rows, batch_size):
        end = min(start + batch_size, total_rows)
        length = end - start

        rng = np.random.default_rng()
        keys = pa.array([str(i) for i in range(start, end)], type=pa.utf8())
        arrays: list[pa.Array] = [keys]
        for col_idx in range(num_cols):
            values = rng.random(length, dtype=np.float32)
            arrays.append(pa.array(values, type=pa.float32()))

        yield pa.RecordBatch.from_arrays(arrays, schema=schema)


def generate_random_keys(count: int, max_key: int) -> list[str]:
    rng = np.random.default_rng()
    return [str(k) for k in rng.integers(0, max_key, size=count)]
