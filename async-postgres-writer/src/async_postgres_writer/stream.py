import asyncio
import hashlib
import json
import re
from collections.abc import AsyncIterable, AsyncIterator
from decimal import Decimal, DecimalException
from typing import Literal, overload

from .errors import WriterError

_INVALID_STRING = re.compile(r"[\x00\ud800-\udfff]")


def _object(pairs: list[tuple[str, object]]) -> dict:
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("Duplicate JSON key")
        result[key] = value
    return result


def _constant(value: str) -> None:
    raise ValueError("Non-finite JSON number")


def _validate(line: bytes, decoded: bool) -> str | dict[str, object]:
    try:
        text = line.decode("utf-8")
        value = json.loads(
            text, object_pairs_hook=_object, parse_constant=_constant, parse_float=Decimal
        )
        if not isinstance(value, dict):
            raise ValueError("Expected an object")
        pending = [value]
        while pending:
            item = pending.pop()
            if isinstance(item, dict):
                pending.extend(item.keys())
                pending.extend(item.values())
            elif isinstance(item, list):
                pending.extend(item)
            elif isinstance(item, str) and (
                # ASCII не содержит суррогатов: пропускаем regex, но NUL проверяем всегда.
                "\x00" in item or (not item.isascii() and _INVALID_STRING.search(item))
            ):
                raise ValueError("String unsupported by JSONB")
    except (ValueError, UnicodeError, RecursionError, DecimalException) as exc:
        raise WriterError(400, "invalid_stream", "Each line must be a valid JSON object") from exc
    return value if decoded else text


class NDJSONStream:
    def __init__(
        self,
        chunks: AsyncIterable[bytes],
        *,
        max_line_bytes: int,
        max_upload_bytes: int,
        read_timeout_seconds: float,
    ) -> None:
        self.chunks = chunks
        self.max_line_bytes = max_line_bytes
        self.max_upload_bytes = max_upload_bytes
        self.read_timeout_seconds = read_timeout_seconds
        self.row_count = 0
        self.byte_count = 0
        self._digest = hashlib.sha256()

    @property
    def sha256(self) -> str:
        return self._digest.hexdigest()

    def __aiter__(self) -> AsyncIterator[tuple[int, str]]:
        return self._iterate(False)

    def decoded_records(self) -> AsyncIterator[tuple[int, dict[str, object]]]:
        """Consume the stream as validated JSON objects instead of original text."""
        return self._iterate(True)

    @overload
    def _iterate(self, decoded: Literal[False]) -> AsyncIterator[tuple[int, str]]: ...

    @overload
    def _iterate(self, decoded: Literal[True]) -> AsyncIterator[tuple[int, dict[str, object]]]: ...

    async def _iterate(self, decoded: bool) -> AsyncIterator[tuple[int, str | dict[str, object]]]:
        pending = bytearray()
        source = aiter(self.chunks)
        while True:
            try:
                async with asyncio.timeout(self.read_timeout_seconds):
                    chunk = await anext(source)
            except StopAsyncIteration:
                break
            except TimeoutError as exc:
                raise WriterError(
                    408, "stream_timeout", "Timed out waiting for stream data"
                ) from exc
            self.byte_count += len(chunk)
            if self.byte_count > self.max_upload_bytes:
                raise WriterError(413, "upload_too_large", "Upload exceeds the byte limit")
            self._digest.update(chunk)
            start = 0
            while start < len(chunk):
                end = chunk.find(b"\n", start)
                stop = len(chunk) if end < 0 else end
                # Retain at most one line plus a possible CR from a split CRLF.
                if len(pending) + stop - start > self.max_line_bytes + 1:
                    raise WriterError(413, "line_too_large", "A record exceeds the byte limit")
                pending.extend(chunk[start:stop])
                if end < 0:
                    break
                yield self._record(pending, decoded)
                pending.clear()
                start = end + 1
        if pending:
            yield self._record(pending, decoded)
        if self.row_count == 0:
            raise WriterError(400, "invalid_stream", "Upload must contain at least one JSON object")

    def _record(self, line: bytearray, decoded: bool) -> tuple[int, str | dict[str, object]]:
        if line.endswith(b"\r"):
            line = line[:-1]
        if len(line) > self.max_line_bytes:
            raise WriterError(413, "line_too_large", "A record exceeds the byte limit")
        record = _validate(bytes(line), decoded)
        self.row_count += 1
        return self.row_count, record
