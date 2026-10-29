"""Convert supported Python inputs to bounded Arrow batches."""

import sys
from collections.abc import Iterable, Iterator, Mapping
from itertools import chain, islice
from typing import Any

import pyarrow as pa


def as_batches(
    data: Any, *, schema: pa.Schema | None = None, batch_size: int = 8192
) -> Iterator[pa.RecordBatch]:
    """Accept Arrow, pandas DataFrame, or an iterable of records/batches.

    Python records require an explicit schema to avoid inference changing across chunks.
    pandas is optional; its index is not persisted.
    """
    if isinstance(batch_size, bool) or not isinstance(batch_size, int) or batch_size <= 0:
        raise ValueError("batch_size must be a positive integer")
    if schema is not None and not isinstance(schema, pa.Schema):
        raise TypeError("schema must be a pyarrow.Schema")
    if isinstance(data, (pa.Table, pa.RecordBatch)):
        if schema is not None:
            if data.schema.names != schema.names:
                raise ValueError("Input columns must match schema names and order")
            data = data.cast(schema, safe=True)
        if isinstance(data, pa.Table):
            yield from data.to_batches(max_chunksize=batch_size)
        else:
            for offset in range(0, data.num_rows, batch_size):
                yield data.slice(offset, batch_size)
        return
    # Avoid making pandas a mandatory dependency for Arrow-only applications.
    pandas = sys.modules.get("pandas")
    if pandas is not None and isinstance(data, pandas.DataFrame):
        if schema is None:
            # Explicit schema avoids full-column temporary allocations during inference.
            schema = pa.Schema.from_pandas(data, preserve_index=False).remove_metadata()
        if list(data.columns) != schema.names:
            raise ValueError("DataFrame columns must match schema names and order")
        for offset in range(0, len(data), batch_size):
            chunk = pa.Table.from_pandas(
                data.iloc[offset : offset + batch_size],
                schema=schema,
                preserve_index=False,
                safe=True,
            ).replace_schema_metadata(schema.metadata)
            yield from chunk.to_batches(max_chunksize=batch_size)
        return
    if isinstance(data, (str, bytes, Mapping)) or not isinstance(data, Iterable):
        raise TypeError("Expected Arrow data, DataFrame, or iterable of batches/records")
    iterator = iter(data)
    first = next(iterator, None)
    if first is None:
        return
    items = chain((first,), iterator)
    if isinstance(first, pa.RecordBatch):
        for batch in items:
            if not isinstance(batch, pa.RecordBatch):
                raise TypeError("Cannot mix records and Arrow batches")
            yield from as_batches(batch, schema=schema, batch_size=batch_size)
        return
    if schema is None:
        raise ValueError("Python records require an explicit pyarrow.Schema")
    while rows := list(islice(items, batch_size)):
        for row in rows:
            if not isinstance(row, Mapping) or set(row) != set(schema.names):
                raise ValueError("Each Python record must contain exactly the schema columns")
        # Direct from_pylist(schema=...) can silently truncate Python floats to integers.
        batch = pa.RecordBatch.from_pylist(rows).select(schema.names).cast(schema, safe=True)
        for field, column in zip(schema, batch.columns, strict=True):
            if not field.nullable and column.null_count:
                raise ValueError(f"Null in non-nullable column: {field.name}")
        yield batch
