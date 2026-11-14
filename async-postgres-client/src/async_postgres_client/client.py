import json
import re
from collections.abc import AsyncIterable, AsyncIterator
from contextlib import aclosing, asynccontextmanager
from datetime import datetime
from decimal import Decimal, InvalidOperation
from types import TracebackType
from typing import Self
from uuid import UUID

import httpx
from anyio import CancelScope

from .models import CursorPage, JSONValue, OffsetPage, Receipt, ResultRow

_NDJSON = "application/x-ndjson"
_JSON = "application/json"
_MAX_POSITION = 2**63 - 1
_PAGE_BYTES = 16 * 1024**2
_METADATA_BYTES = 64 * 1024


class ProtocolError(Exception):
    """The response does not match the API contract."""


class TransportError(Exception):
    """The HTTP exchange failed; a PUT may already have committed."""


class HTTPError(Exception):
    """A non-success status; server details are retained when available."""

    def __init__(self, status_code: int, body: dict[str, JSONValue]) -> None:
        self.status_code = status_code
        error, message, detail = body.get("error"), body.get("message"), body.get("detail")
        self.error = error if isinstance(error, str) else None
        self.message = message if isinstance(message, str) else None
        self.detail = detail if isinstance(detail, (str, list)) else None
        super().__init__(f"HTTP {status_code}" + (f": {self.error}" if self.error else ""))


def _decode_integer(value: str) -> int:
    # PostgreSQL numeric can exceed Python's decimal string conversion limit.
    return int(value) if len(value) <= 640 else int(Decimal(value))


def _reject_constant(value: str) -> None:
    raise ValueError(f"Invalid JSON constant: {value}")


def _object(raw: bytes | bytearray) -> dict[str, JSONValue]:
    try:
        value = json.loads(
            raw.decode("utf-8"),
            parse_float=Decimal,
            parse_int=_decode_integer,
            parse_constant=_reject_constant,
        )
    except (ValueError, InvalidOperation, RecursionError):
        raise ProtocolError("Expected a UTF-8 JSON object") from None
    if not isinstance(value, dict):
        raise ProtocolError("Expected a JSON object")
    return value


def _int_field(value: JSONValue, name: str, minimum: int = 0, maximum: int = _MAX_POSITION) -> int:
    if type(value) is not int or not minimum <= value <= maximum:
        raise ProtocolError(f"Invalid {name}")
    return value


def _str_field(value: JSONValue, name: str) -> str:
    if not isinstance(value, str):
        raise ProtocolError(f"Invalid {name}")
    return value


def _require_upload_id(value: JSONValue, expected: UUID) -> None:
    try:
        if UUID(_str_field(value, "upload_id")) == expected:
            return
    except ValueError:
        pass
    raise ProtocolError("Unexpected upload_id")


def _receipt(obj: dict[str, JSONValue], upload_id: UUID, *, reused: bool) -> Receipt:
    _require_upload_id(obj.get("upload_id"), upload_id)
    sha256 = _str_field(obj.get("sha256"), "sha256")
    if not re.fullmatch(r"[0-9a-f]{64}", sha256):
        raise ProtocolError("Invalid sha256")
    try:
        committed_at = datetime.fromisoformat(_str_field(obj.get("committed_at"), "committed_at"))
    except ValueError:
        raise ProtocolError("Invalid committed_at") from None
    if committed_at.tzinfo is None or obj.get("reused") is not reused:
        raise ProtocolError("Invalid receipt timestamp or reuse flag")
    return Receipt(
        upload_id,
        sha256,
        _int_field(obj.get("row_count"), "row_count"),
        _int_field(obj.get("byte_count"), "byte_count"),
        committed_at,
        reused,
    )


def _row(obj: JSONValue) -> ResultRow:
    if not isinstance(obj, dict) or not isinstance(data := obj.get("data"), dict):
        raise ProtocolError("Expected a result row with object data")
    return ResultRow(_int_field(obj.get("row_number"), "row_number", 1), data)


def _upload_path(upload_id: UUID) -> str:
    if not isinstance(upload_id, UUID):
        raise TypeError("upload_id must be a UUID")
    return f"v1/uploads/{upload_id}"


def _response_type(response: httpx.Response, media_type: str) -> None:
    if response.headers.get("Content-Type", "").partition(";")[0].strip().lower() != media_type:
        raise ProtocolError(f"Expected a {media_type} response")
    if response.headers.get("Content-Encoding", "identity").strip().lower() != "identity":
        raise ProtocolError("Expected an uncompressed response")


async def _bytes(response: httpx.Response) -> AsyncIterator[bytes]:
    try:
        async for chunk in response.aiter_bytes():
            yield chunk
    except httpx.RequestError as exc:
        raise TransportError("Could not finish reading the HTTP response") from exc


async def _json_body(response: httpx.Response, max_bytes: int) -> dict[str, JSONValue]:
    _response_type(response, _JSON)
    body = bytearray()
    async for chunk in _bytes(response):
        if len(body) + len(chunk) > max_bytes:
            raise ProtocolError("JSON response exceeds its byte limit")
        body.extend(chunk)
    return _object(body)


def _row_count(response: httpx.Response) -> int:
    value = response.headers.get("X-Row-Count", "")
    if not re.fullmatch(r"[0-9]{1,19}", value) or int(value) > _MAX_POSITION:
        raise ProtocolError("Invalid X-Row-Count")
    return int(value)


async def _stream_rows(
    response: httpx.Response, expected_rows: int, max_record_bytes: int
) -> AsyncIterator[ResultRow]:
    record = bytearray()
    count = last_row = 0
    async for chunk in _bytes(response):
        start = 0
        while start < len(chunk):
            end = chunk.find(b"\n", start)
            stop = len(chunk) if end < 0 else end
            if len(record) + stop - start > max_record_bytes:
                raise ProtocolError("NDJSON record exceeds its byte limit")
            record.extend(memoryview(chunk)[start:stop])
            if end < 0:
                break
            row = _row(_object(record))
            record.clear()
            count += 1
            if count > expected_rows or row.row_number <= last_row:
                raise ProtocolError("Unexpected row count or order in NDJSON response")
            last_row = row.row_number
            yield row
            start = end + 1
    if record or count != expected_rows:
        raise ProtocolError("Incomplete NDJSON response")


class AsyncPostgresClient:
    """Reusable asynchronous client for OpenAPI contract v1.0.0."""

    def __init__(
        self,
        base_url: str,
        token: str,
        *,
        timeout: float | httpx.Timeout = 30.0,
        max_record_bytes: int = _PAGE_BYTES,
        transport: httpx.AsyncBaseTransport | None = None,
    ) -> None:
        if not token or not all(0x21 <= ord(char) <= 0x7E for char in token):
            raise ValueError(
                "token must be nonempty and contain only printable ASCII without spaces"
            )
        url = httpx.URL(base_url)
        if (
            url.scheme not in ("http", "https")
            or not url.host
            or url.userinfo
            or url.query
            or url.fragment
        ):
            raise ValueError(
                "base_url must be an HTTP(S) URL without credentials, query or fragment"
            )
        if type(max_record_bytes) is not int or max_record_bytes < 1:
            raise ValueError("max_record_bytes must be a positive integer")
        self._max_record_bytes = max_record_bytes
        self._client = httpx.AsyncClient(
            base_url=url,
            headers={"Authorization": f"Bearer {token}", "Accept-Encoding": "identity"},
            timeout=timeout,
            follow_redirects=False,
            transport=transport,
        )

    async def __aenter__(self) -> Self:
        await self._client.__aenter__()
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        with CancelScope(shield=True):
            await self._client.__aexit__(exc_type, exc_value, traceback)

    @asynccontextmanager
    async def _request(
        self,
        method: str,
        path: str,
        expected: tuple[int, ...],
        *,
        content: AsyncIterable[bytes] | None = None,
        params: dict[str, int] | None = None,
    ) -> AsyncIterator[httpx.Response]:
        headers = {"Accept": _NDJSON} if path.endswith("/stream") else {"Accept": _JSON}
        if content is not None:
            headers["Content-Type"] = f"{_NDJSON}; charset=utf-8"
        request = self._client.build_request(
            method, path, params=params, headers=headers, content=content
        )
        try:
            response = await self._client.send(request, stream=True)
        except httpx.RequestError as exc:
            raise TransportError("Could not send the HTTP request or read its headers") from exc
        try:
            if not response.is_success:
                try:
                    error_body = await _json_body(response, _METADATA_BYTES)
                except (ProtocolError, TransportError):
                    error_body = {}
                raise HTTPError(response.status_code, error_body)
            if response.status_code not in expected:
                raise ProtocolError(f"Unexpected HTTP success status: {response.status_code}")
            yield response
        finally:
            with CancelScope(shield=True):
                await response.aclose()

    async def upload_ndjson(self, upload_id: UUID, content: AsyncIterable[bytes]) -> Receipt:
        """Send bytes once; the caller owns the input iterable and any files it uses."""
        async with self._request(
            "PUT", _upload_path(upload_id), (200, 201), content=content
        ) as response:
            return _receipt(
                await _json_body(response, _METADATA_BYTES),
                upload_id,
                reused=response.status_code == 200,
            )

    async def get_upload_receipt(self, upload_id: UUID) -> Receipt:
        async with self._request("GET", _upload_path(upload_id), (200,)) as response:
            return _receipt(await _json_body(response, _METADATA_BYTES), upload_id, reused=False)

    async def list_upload_rows(
        self,
        upload_id: UUID,
        *,
        limit: int = 100,
        offset: int | None = None,
        after_row_number: int | None = None,
    ) -> OffsetPage | CursorPage:
        if type(limit) is not int or not 1 <= limit <= 1000:
            raise ValueError("limit must be between 1 and 1000")
        if offset is not None and after_row_number is not None:
            raise ValueError("Pass either offset or after_row_number")
        params = {"limit": limit}
        for name, value in (("offset", offset), ("after_row_number", after_row_number)):
            if value is not None:
                if type(value) is not int or not 0 <= value <= _MAX_POSITION:
                    raise ValueError(f"{name} must be a nonnegative int64")
                params[name] = value
        async with self._request(
            "GET", f"{_upload_path(upload_id)}/rows", (200,), params=params
        ) as response:
            obj = await _json_body(response, _PAGE_BYTES)
        _require_upload_id(obj.get("upload_id"), upload_id)
        row_count = _int_field(obj.get("row_count"), "row_count")
        if _int_field(obj.get("limit"), "limit", 1, 1000) != limit:
            raise ProtocolError("Unexpected page limit")
        raw_rows = obj.get("rows")
        if not isinstance(raw_rows, list) or len(raw_rows) > min(row_count, limit):
            raise ProtocolError("Invalid page rows")
        rows = tuple(_row(raw) for raw in raw_rows)
        previous = after_row_number or 0
        for row in rows:
            if row.row_number <= previous:
                raise ProtocolError("Unexpected row order in page")
            previous = row.row_number
        if after_row_number is None:
            actual_offset = _int_field(obj.get("offset"), "offset")
            if (
                actual_offset != (offset or 0)
                or {"after_row_number", "next_after_row_number"} & obj.keys()
            ):
                raise ProtocolError("Unexpected page offset")
            return OffsetPage(upload_id, row_count, limit, rows, actual_offset)
        actual_after = _int_field(obj.get("after_row_number"), "after_row_number")
        if (
            actual_after != after_row_number
            or "next_after_row_number" not in obj
            or "offset" in obj
        ):
            raise ProtocolError("Invalid page cursor")
        next_cursor = obj["next_after_row_number"]
        if next_cursor is not None:
            next_cursor = _int_field(next_cursor, "next_after_row_number", after_row_number + 1)
            if not rows or next_cursor != rows[-1].row_number:
                raise ProtocolError("Invalid next page cursor")
        return CursorPage(upload_id, row_count, limit, rows, actual_after, next_cursor)

    @asynccontextmanager
    async def stream_upload_rows(self, upload_id: UUID) -> AsyncIterator[AsyncIterator[ResultRow]]:
        """Yield rows provisionally until the iterator reaches EOF and verifies the row count."""
        async with self._request(
            "GET", f"{_upload_path(upload_id)}/rows/stream", (200,)
        ) as response:
            _response_type(response, _NDJSON)
            stream = _stream_rows(response, _row_count(response), self._max_record_bytes)
            async with aclosing(stream) as rows:
                yield rows
