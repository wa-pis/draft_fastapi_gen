import argparse
import json
import os
from collections.abc import Iterable, Iterator, Sequence
from contextlib import contextmanager
from itertools import batched
from pathlib import Path
from tempfile import NamedTemporaryFile
from typing import TextIO

TABLE = "public.postgres_io_example"
COLUMNS = ("id", "name", "value")
type Row = tuple[int, str, int]


def parse_args(*, with_async_flag: bool = False) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Move NDJSON rows between a file and PostgreSQL")
    parser.add_argument(
        "action", choices=("batch-write", "batch-read", "stream-write", "stream-read")
    )
    parser.add_argument("path", type=Path, help="Input file for writes or output file for reads")
    parser.add_argument(
        "--batch-size", type=int, default=1000, help="Rows per batch, page or fetch"
    )
    if with_async_flag:
        parser.add_argument("--async", dest="use_async", action="store_true")
    args = parser.parse_args()
    if args.batch_size <= 0:
        parser.error("--batch-size must be positive")
    args.dsn = os.environ.get("POSTGRES_DSN", "")
    if not args.dsn.startswith("postgresql://"):
        parser.error("set POSTGRES_DSN to a postgresql:// URI")
    return args


def rows(path: Path) -> Iterator[Row]:
    with path.open(encoding="utf-8") as source:
        for number, line in enumerate(source, 1):
            try:
                item = json.loads(line)
                if not isinstance(item, dict) or set(item) != set(COLUMNS):
                    raise ValueError("expected an object with id, name and value")
                identifier, name, value = (item[column] for column in COLUMNS)
                if (
                    type(identifier) is not int
                    or not -(2**63) <= identifier < 2**63
                    or not isinstance(name, str)
                    or type(value) is not int
                    or not -(2**31) <= value < 2**31
                ):
                    raise ValueError("expected a bigint id, text name and integer value")
            except ValueError as exc:
                raise ValueError(f"{path}:{number}: {exc}") from exc
            yield identifier, name, value


def batches(source: Iterable[Row], size: int) -> Iterator[list[Row]]:
    for chunk in batched(source, size):
        yield list(chunk)


@contextmanager
def atomic_output(path: Path) -> Iterator[TextIO]:
    with NamedTemporaryFile(mode="w", encoding="utf-8", dir=path.parent, delete=False) as output:
        temporary = Path(output.name)
        try:
            yield output
            output.close()
            temporary.replace(path)
        finally:
            temporary.unlink(missing_ok=True)


def write_row(output: TextIO, row: Sequence[int | str]) -> None:
    output.write(json.dumps(dict(zip(COLUMNS, row, strict=True)), ensure_ascii=False) + "\n")
