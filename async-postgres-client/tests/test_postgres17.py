"""Exercise the installed client over real HTTP against the PostgreSQL writer."""

import os
from decimal import Decimal
from hashlib import sha256
from uuid import uuid4

import httpx
import pytest

from async_postgres_client import AsyncPostgresClient, CursorPage, HTTPError, OffsetPage

pytestmark = pytest.mark.integration
NDJSON = "application/x-ndjson"
UPLOAD = "/v1/uploads/{upload_id}"


@pytest.fixture
def live_api():
    url = os.getenv("WRITER_TEST_API_URL")
    if not url:
        pytest.skip("Set WRITER_TEST_API_URL for tests against the PostgreSQL writer")
    return url, os.environ["WRITER_API_TOKEN"]


async def test_server_contract_matches_client_operations(live_api):
    url, _ = live_api
    async with httpx.AsyncClient(base_url=url) as http:
        response = await http.get("/openapi.json")
    response.raise_for_status()
    spec = response.json()
    assert spec["openapi"].startswith("3.1.")
    assert spec["info"]["version"] == "1.0.0"
    assert spec["components"]["securitySchemes"]["bearerAuth"] == {
        "type": "http",
        "scheme": "bearer",
    }
    expected = {
        (UPLOAD, "put"): ("uploadNdjson", {200, 201, 400, 401, 408, 409, 413, 415, 422, 503}),
        (UPLOAD, "get"): ("getUploadReceipt", {200, 401, 404, 422, 503}),
        (UPLOAD + "/rows", "get"): ("listUploadRows", {200, 401, 404, 413, 422, 503}),
        (UPLOAD + "/rows/stream", "get"): ("streamUploadRows", {200, 401, 404, 422, 503}),
    }
    for (path, method), (operation_id, statuses) in expected.items():
        route = spec["paths"][path]
        assert route["parameters"] == [{"$ref": "#/components/parameters/UploadId"}]
        op = route[method]
        assert op["operationId"] == operation_id
        assert set(map(int, op["responses"])) == statuses
        assert op.get("security", spec["security"]) == [{"bearerAuth": []}]
    paths = spec["paths"]
    assert NDJSON in paths[UPLOAD]["put"]["requestBody"]["content"]
    page = paths[UPLOAD + "/rows"]["get"]
    assert {param["name"] for param in page["parameters"]} == {
        "limit",
        "offset",
        "after_row_number",
    }
    assert "application/json" in page["responses"]["200"]["content"]
    stream = paths[UPLOAD + "/rows/stream"]["get"]["responses"]["200"]
    assert NDJSON in stream["content"]
    assert stream["headers"]["X-Row-Count"]["required"]
    assert "Cache-Control" in stream["headers"]


async def test_upload_receipt_pages_and_stream(live_api):
    url, token = live_api
    upload = uuid4()
    wide_text = "ёж" * 50_000  # One UTF-8 line spans multiple server chunks.
    precise_amount = Decimal("123.4567890123456789012345678")
    body = (
        f'{{"amount":{precise_amount},"text":"{wide_text}","huge":1e400}}\n'
        '{"line":2}\n{"line":3}\n'
    ).encode()

    async def upload_parts(data=body):
        for start in range(0, len(data), 8191):
            yield data[start : start + 8191]

    async with AsyncPostgresClient(url, token) as api:
        receipt = await api.upload_ndjson(upload, upload_parts())
        assert receipt.upload_id == upload
        assert (receipt.row_count, receipt.byte_count, receipt.reused) == (3, len(body), False)
        assert receipt.sha256 == sha256(body).hexdigest()
        assert await api.get_upload_receipt(upload) == receipt
        assert (await api.upload_ndjson(upload, upload_parts())).reused is True
        with pytest.raises(HTTPError) as conflict:
            await api.upload_ndjson(upload, upload_parts(b'{"different":true}\n'))
        assert (conflict.value.status_code, conflict.value.error) == (409, "upload_conflict")

        legacy = await api.list_upload_rows(upload, limit=1, offset=0)
        first = await api.list_upload_rows(upload, limit=1, after_row_number=0)
        assert isinstance(legacy, OffsetPage) and legacy.offset == 0
        assert isinstance(first, CursorPage) and first.next_after_row_number == 1
        assert first.rows == legacy.rows
        assert first.rows[0].data["amount"] == precise_amount
        assert first.rows[0].data["huge"] == 10**400
        last = await api.list_upload_rows(upload, limit=2, after_row_number=1)
        assert isinstance(last, CursorPage) and last.next_after_row_number is None

        async with api.stream_upload_rows(upload) as stream:
            assert [row async for row in stream] == [*first.rows, *last.rows]
        async with api.stream_upload_rows(upload) as stream:
            assert await anext(stream) == first.rows[0]
        assert await api.get_upload_receipt(upload) == receipt

        invalid = uuid4()
        with pytest.raises(HTTPError) as rejected:
            await api.upload_ndjson(invalid, upload_parts(b'{"valid":true}\n{"incomplete":}\n'))
        assert rejected.value.status_code == 400
        with pytest.raises(HTTPError) as missing:
            await api.get_upload_receipt(invalid)
        assert missing.value.status_code == 404

    async with AsyncPostgresClient(url, "invalid-token") as api:
        with pytest.raises(HTTPError) as unauthorized:
            await api.get_upload_receipt(upload)
        assert unauthorized.value.status_code == 401
