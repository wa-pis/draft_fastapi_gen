import asyncio
from datetime import UTC, datetime
from decimal import Decimal
from uuid import UUID

import anyio
import httpx
import pytest

from async_postgres_client import (
    AsyncPostgresClient,
    CursorPage,
    HTTPError,
    OffsetPage,
    ProtocolError,
    Receipt,
    ResultRow,
    TransportError,
)

ID = UUID(int=1)
PATH = f"/v1/uploads/{ID}"
TOKEN = "test-token"
LINE = b'{"row_number":1,"data":{}}\n'
RECEIPT = {
    "upload_id": str(ID),
    "sha256": "a" * 64,
    "row_count": 2,
    "byte_count": 7,
    "committed_at": "2026-11-14T12:00:00Z",
    "reused": False,
}


def client(transport, **options):
    return AsyncPostgresClient("https://testing.local", TOKEN, transport=transport, **options)


class UploadTransport(httpx.AsyncBaseTransport):
    def __init__(self):
        self.methods = []

    async def handle_async_request(self, request):
        self.methods.append(request.method)
        assert request.url.path == PATH
        assert request.headers["Authorization"] == f"Bearer {TOKEN}"
        if request.method == "PUT":
            assert request.headers["Content-Type"].split(";")[0] == "application/x-ndjson"
            assert await anext(aiter(request.stream)) == b"{}\n"
            raise httpx.ReadError("Lost response")
        return httpx.Response(200, json=RECEIPT)


async def test_upload_is_lazy_and_lost_response_does_not_replay():
    async def body():
        yield b"{}\n"
        pytest.fail("The transport did not ask for the next chunk")

    transport = UploadTransport()
    async with client(transport) as api:
        with pytest.raises(TransportError):
            await api.upload_ndjson(ID, body())
        assert transport.methods == ["PUT"]
        assert await api.get_upload_receipt(ID) == Receipt(
            ID, "a" * 64, 2, 7, datetime(2026, 11, 14, 12, tzinfo=UTC), False
        )
    assert transport.methods == ["PUT", "GET"]


@pytest.mark.parametrize("status", [201, 200])
async def test_upload_sends_original_bytes_and_returns_receipt(status):
    async def body():
        yield b'{"value":'
        yield b"1.2300}\n"

    def reply(request):
        assert request.content == b'{"value":1.2300}\n'
        return httpx.Response(status, json={**RECEIPT, "reused": status == 200})

    async with client(httpx.MockTransport(reply)) as api:
        receipt = await api.upload_ndjson(ID, body())
    assert receipt.upload_id == ID and receipt.reused is (status == 200)


async def test_upload_does_not_follow_redirect_or_forward_token():
    def reply(request):
        assert request.url.host == "testing.local"
        return httpx.Response(307, headers={"Location": "https://unexpected.local/upload"})

    async def body():
        yield b"{}\n"

    async with client(httpx.MockTransport(reply)) as api:
        with pytest.raises(HTTPError) as caught:
            await api.upload_ndjson(ID, body())
    assert caught.value.status_code == 307


@pytest.mark.parametrize(
    "status,payload,code,message,detail",
    [
        (
            503,
            {"error": "commit_unknown", "message": "Unknown commit"},
            "commit_unknown",
            "Unknown commit",
            None,
        ),
        (404, {"detail": "No receipt"}, None, None, "No receipt"),
        (
            409,
            {"error": "upload_conflict", "message": "Other data"},
            "upload_conflict",
            "Other data",
            None,
        ),
        (
            422,
            {"detail": [{"loc": ["query", "limit"], "msg": "Invalid", "type": "int_parsing"}]},
            None,
            None,
            [{"loc": ["query", "limit"], "msg": "Invalid", "type": "int_parsing"}],
        ),
        (502, None, None, None, None),
    ],
)
async def test_error_status_and_server_details_survive(status, payload, code, message, detail):
    def reply(_):
        if payload is None:
            return httpx.Response(status, text="<html>Upstream error</html>")
        return httpx.Response(status, json=payload)

    async def body():
        yield b"{}\n"

    async with client(httpx.MockTransport(reply)) as api:
        with pytest.raises(HTTPError) as caught:
            if status in {409, 503}:
                await api.upload_ndjson(ID, body())
            else:
                await api.get_upload_receipt(ID)
    error = caught.value
    assert (error.status_code, error.error, error.detail) == (status, code, detail)
    if message is not None:
        assert error.message == message


async def test_pages_keep_position_and_exact_numbers():
    wire = (
        f'{{"upload_id":"{ID}","row_count":10,"limit":1,'
        '"rows":[{"row_number":7,"data":{"amount":123.4567890123456789012345,"huge":1e400}}],'
    ).encode()
    queries = []

    def reply(request):
        assert request.url.path == PATH + "/rows"
        query = dict(request.url.params)
        queries.append(query)
        if "after_row_number" in query:
            return httpx.Response(
                200,
                content=wire + b'"after_row_number":2,"next_after_row_number":7}',
                headers={"Content-Type": "application/json"},
            )
        return httpx.Response(
            200, content=wire + b'"offset":0}', headers={"Content-Type": "application/json"}
        )

    async with client(httpx.MockTransport(reply)) as api:
        assert isinstance(await api.list_upload_rows(ID, limit=1), OffsetPage)
        assert isinstance(await api.list_upload_rows(ID, limit=1, offset=0), OffsetPage)
        page = await api.list_upload_rows(ID, limit=1, after_row_number=2)
        assert isinstance(page, CursorPage) and page.next_after_row_number == 7
        assert page.rows == (
            ResultRow(
                7, {"amount": Decimal("123.4567890123456789012345"), "huge": Decimal("1e400")}
            ),
        )
        with pytest.raises(ValueError):
            await api.list_upload_rows(ID, offset=0, after_row_number=0)
    assert queries == [
        {"limit": "1"},
        {"limit": "1", "offset": "0"},
        {"limit": "1", "after_row_number": "2"},
    ]


class ResponseBody(httpx.AsyncByteStream):
    def __init__(self, chunks, *, disconnect=False, block=False):
        self.chunks = chunks
        self.disconnect = disconnect
        self.block = block
        self.waiting = asyncio.Event()
        self.closed = False
        self.read_chunks = 0

    async def __aiter__(self):
        for chunk in self.chunks:
            self.read_chunks += 1
            yield chunk
        if self.disconnect:
            raise httpx.RemoteProtocolError("Incomplete HTTP message")
        if self.block:
            self.waiting.set()
            await asyncio.Future()

    async def aclose(self):
        await asyncio.sleep(0)
        self.closed = True


async def test_oversized_error_body_does_not_hide_http_status():
    body = ResponseBody([b" " * (64 * 1024 + 1), b'{"detail":"Too late"}'])

    def reply(_):
        return httpx.Response(503, stream=body, headers={"Content-Type": "application/json"})

    async with client(httpx.MockTransport(reply)) as api:
        with pytest.raises(HTTPError) as caught:
            await api.get_upload_receipt(ID)
    assert caught.value.status_code == 503 and caught.value.detail is None
    assert body.closed and body.read_chunks == 1


def export(body, count="1", media_type="application/x-ndjson"):
    def reply(request):
        assert request.url.path == PATH + "/rows/stream"
        headers = {"Content-Type": media_type}
        if count is not None:
            headers["X-Row-Count"] = count
        return httpx.Response(200, stream=body, headers=headers)

    return httpx.MockTransport(reply)


async def test_export_reassembles_large_utf8_record_and_yields_before_eof():
    value = "я" * 70_000
    line = f'{{"row_number":8,"data":{{"text":"{value}","amount":1.00000000000000001}}}}\n'.encode()
    split = line.index("я".encode()) + 1
    body = ResponseBody(
        [line[:split], line[split:131_072], line[131_072:], LINE.replace(b":1", b":9")]
    )
    async with (
        client(export(body, "2")) as api,
        api.stream_upload_rows(ID) as rows,
    ):
        assert await anext(rows) == ResultRow(
            8, {"text": value, "amount": Decimal("1.00000000000000001")}
        )
        assert body.read_chunks == 3
        assert [row async for row in rows] == [ResultRow(9, {})]
    assert body.closed


@pytest.mark.parametrize(
    "parts,count,media_type,limit",
    [
        ([LINE], None, "application/x-ndjson", 1024),
        ([LINE], "NaN", "application/x-ndjson", 1024),
        ([LINE], "-1", "application/x-ndjson", 1024),
        ([LINE], "1", "application/json", 1024),
        ([LINE], "2", "application/x-ndjson", 1024),
        ([LINE], "0", "application/x-ndjson", 1024),
        ([LINE[:-1]], "1", "application/x-ndjson", 1024),
        ([b"invalid\n"], "1", "application/x-ndjson", 1024),
        ([b'{"row_number":1,"data":{"text":"\xff"}}\n'], "1", "application/x-ndjson", 1024),
        ([LINE[:12], LINE[12:]], "1", "application/x-ndjson", len(LINE) - 2),
    ],
)
async def test_invalid_or_incomplete_exports_raise(parts, count, media_type, limit):
    body = ResponseBody(parts)
    async with client(export(body, count, media_type), max_record_bytes=limit) as api:
        with pytest.raises(ProtocolError):
            async with api.stream_upload_rows(ID) as rows:
                _ = [row async for row in rows]
    assert body.closed


async def test_early_exit_closes_response_without_consuming_remaining_rows():
    body = ResponseBody([LINE], block=True)
    async with client(export(body, "2")) as api, api.stream_upload_rows(ID) as rows:
        assert await anext(rows) == ResultRow(1, {})
    assert body.closed and not body.waiting.is_set()


async def test_disconnect_after_valid_prefix_is_not_success_or_retried():
    body = ResponseBody([LINE], disconnect=True)
    async with client(export(body)) as api, api.stream_upload_rows(ID) as rows:
        assert await anext(rows) == ResultRow(1, {})
        with pytest.raises(TransportError):
            await anext(rows)
    assert body.closed and body.read_chunks == 1


async def test_cancellation_closes_response_while_waiting_for_next_chunk():
    body = ResponseBody([LINE], block=True)

    async def consume(api):
        async with api.stream_upload_rows(ID) as rows:
            async for _ in rows:
                pass

    async with client(export(body, "2")) as api:
        with anyio.fail_after(3):
            async with anyio.create_task_group() as tasks:
                tasks.start_soon(consume, api)
                await body.waiting.wait()
                tasks.cancel_scope.cancel()
        assert body.closed
